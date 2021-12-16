from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link
from ryu.lib import hub
import networkx as nx


class ExampleShortestForwarding(app_manager.RyuApp):
    """docstring for ExampleShortestForwarding"""

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ExampleShortestForwarding, self).__init__(*args, **kwargs)
        self.network = nx.DiGraph()
        self.topology_api_app = self
        self.paths = {}
        self.features = {}
        # 1. 开启一个线程, 定时探测并收集交换机端口的延迟和丢包率
        self.monitor_thread = hub.spawn(self._monitor)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
            记录交换机状态
        """
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        # install the table-miss flow entry.
        match = ofp_parser.OFPMatch()
        actions = [ofp_parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                              ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        # construct flow_mod message and send it.
        inst = [ofp_parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                 actions)]
        mod = ofp_parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(event.EventSwitchEnter, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def get_topology(self, ev):
        # get switches and store them into self.network
        switch_list = get_switch(self.topology_api_app, None)
        switches = [switch.dp.id for switch in switch_list]
        self.network.add_nodes_from(switches)

        # get links and store them into self.network
        links_list = get_link(self.topology_api_app, None)
        links = [(link.src.dpid, link.dst.dpid, {'port': link.src.port_no}) for link in links_list]
        self.network.add_edges_from(links)

        # reverse link.
        links = [(link.dst.dpid, link.src.dpid, {'port': link.dst.port_no}) for link in links_list]
        self.network.add_edges_from(links)

    def get_out_port(self, src, dst, datapath, in_port):
        dpid = datapath.id
        # add link between host and ingress switch.
        if src not in self.network:
            self.network.add_node(src)
            self.network.add_edge(dpid, src, {'port': in_port})
            self.network.add_edge(src, dpid)
            self.paths.setdefault(src, {})

        if dst in self.network:
            # if path is not existed, calculate it and save it.
            if dst not in self.paths[src]:
                path = nx.shortest_path(self.network, src, dst)
                self.paths[src][dst] = path

            # find out_port to next hop.
            path = self.paths[src][dst]
            print("path: ", path)
            next_hop = path[path.index(dpid) + 1]
            out_port = self.network[dpid][next_hop]['port']
        else:
            out_port = datapath.ofproto.OFPP_FLOOD

        return out_port

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        in_port = msg.match['in_port']

        out_port = self.get_out_port(eth.src, eth.dst, datapath, in_port)
        actions = [ofp_parser.OFPActionOutput(out_port)]
        # install flow_mod to avoid packet_in next time.
        if out_port != ofproto.OFPP_FLOOD:
            match = ofp_parser.OFPMatch(in_port=in_port, eth_dst=eth.dst)
            self.add_flow(datapath, 1, match, actions)

        # send packet_out msg to flood packet.
        out = ofp_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port,
            actions=actions)
        datapath.send_msg(out)

    def _monitor(self):
        """ 主动收集 SDN 数据平面的网络特征, 输出出现故障的交换机"""
        while True:
            # 1. 流量收集阶段
            for dp in self.datapaths.values():
                self.packet_out_handler(dp)
            hub.sleep(10)
            # 2. 故障检测阶段
            fault_dpid = self.fault_detection()
            # 3. 更新控制平面
            del_flows = []
            for dpid in self.network.predecessors(fault_dpid):
                del_flows.append((dpid, self.network[dpid][fault_dpid]['port']))

            self.network.remove_node(fault_dpid)
            newpaths = {}
            for src, ds in self.paths.items():
                for dst, ls in ds.items():
                    if fault_dpid not in ls:
                        newpaths[src] = self.paths[src]
            self.paths = newpaths
            # 4. 更新数据平面
            hub.sleep(60)

    def packet_out_handler(self, datapath):
        """ 从该交换机的每个端口下发数据包"""
        pass

    def fault_detection(self):
        """ 返回出现故障的交换机 id, 假设 s7"""
        return '0000000000000007'
