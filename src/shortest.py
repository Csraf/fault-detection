import time
import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER, HANDSHAKE_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.topology.switches import LLDPPacket
from ryu.topology import event, switches
from ryu.base.app_manager import lookup_service_brick
from ryu.topology.api import get_switch, get_link
from ryu.lib import hub


class ExampleShortestForwarding(app_manager.RyuApp):
    """基于最短路径转发的程序模板"""

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ExampleShortestForwarding, self).__init__(*args, **kwargs)
        self.network = nx.DiGraph()
        self.topology_api_app = self
        self.datapaths = {}  # 维护当前数据平面的所有交换机
        self.paths = {}
        self.features = []
        self.echo_delay = {}  # 交换机与控制器之间的延时 {"dpid": "echoDelay"}
        self.port_delay = {}  # 交换机各个端口的延时 {"dpid": {"port1": "portDelay"}}
        self.switches = lookup_service_brick('switches')

        # 1. 开启一个线程, 定时探测并收集交换机端口的延迟和丢包率
        self.monitor_thread = hub.spawn(self._monitor)

    def _monitor(self):
        """ 主动收集 SDN 数据平面的网络特征, 输出出现故障的交换机"""
        while True:
            # 1. 流量收集阶段
            print("下发 echo 报文")
            self.send_echo_request()
            hub.sleep(10)
            print("查看当前收集到的特征")
            self.get_delay_features()
            print("收集到的网络特征", self.features)
            print("执行异常检测算法")
            # # 2. 故障检测阶段
            # fault_dpid = self.fault_detection()
            # # 3. 更新控制平面
            # del_flows = []
            # for dpid in self.network.predecessors(fault_dpid):
            #     del_flows.append((dpid, self.network[dpid][fault_dpid]['port']))
            #
            # self.network.remove_node(fault_dpid)
            # newpaths = {}
            # for src, ds in self.paths.items():
            #     for dst, ls in ds.items():
            #         if fault_dpid not in ls:
            #             newpaths[src] = self.paths[src]
            # self.paths = newpaths
            # # 4. 更新数据平面
            # hub.sleep(60)

    def send_echo_request(self):
        """
        控制器向每个交换机发送 echo 报文, 并记录下发的时间戳
        :return:
        """
        for datapath in self.datapaths.values():
            parser = datapath.ofproto_parser
            echo_req = parser.OFPEchoRequest(datapath, data=bytes("%.12f" % time.time(), encoding="utf8"))
            datapath.send_msg(echo_req)
            hub.sleep(0.5)

    @set_ev_cls(ofp_event.EventOFPEchoReply, [MAIN_DISPATCHER, CONFIG_DISPATCHER, HANDSHAKE_DISPATCHER])
    def echo_reply_handler(self, ev):
        """
        交换机向控制器的echo请求回应报文，收到此报文时，控制器通过当前时间-时间戳，计算出往返时延
        :param ev:
        :return:
        """
        end_timestamp = time.time()
        try:
            delay = end_timestamp - eval(ev.msg.data)
            self.echo_delay[ev.msg.datapath.id] = delay
            print("交换机: ", ev.msg.datapath.id, "与控制器之间的延时 ", delay)
        except Exception as error:
            print("接收 echo 消息出现异常")
            return

    def fault_detection(self):
        """ 返回出现故障的交换机 id, 假设 s7"""
        return '0000000000000007'

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
        """
        当控制器处于配置阶段时， 控制器发送自身特征，交换机响应自身信息。
        :param ev:
        :return:
        """
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
        """
        处理 packetIn 数据包的请求，包含两种情况:
            1. lldp 数据包， 解析出指定交换机的端口的时间戳
            2. 主机转发消息， 下发流表规则
        :param ev:
        :return:
        """
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        dpid = datapath.id
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        dst = eth.dst
        src = eth.src

        try:  # lldp 消息
            src_dpid, src_outport = LLDPPacket.lldp_parse(msg.data)
            dst_dpid = msg.datapath.id
            if self.switches is None:
                self.switches = lookup_service_brick("switches")

            for port in self.switches.ports.keys():
                if src_dpid == port.dpid and src_outport == port.port_no:
                    port_data = self.switches.ports[port]
                    start = port_data.timestamp
                    if start:
                        delay = time.time() - start
                        self.network[src_dpid][dst_dpid]['delay'] = delay
                        # 将网络延时保存到网络拓扑上
                        print("交换机: ", src_dpid, " 的端口 ", port, " 延时为: ", delay)

        except Exception as error:  # 处理主机转发消息
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

    def avg(self, ls):
        """ 求 list 平均值 """
        sum = 0
        for item in ls:
            sum += item
        return sum / len(ls)

    def get_delay_features(self):
        """
        获取交换机各个端口的时延
        :return:
        """
        for datapath in self.datapaths.values():
            src_dpid = datapath.id
            src_echo_delay = self.echo_delay[src_dpid]
            port_delays = []
            for dst_dpid, edge in self.network[src_dpid].items():
                if 'delay' in edge.keys():
                    src_lldp_delay = edge['delay']
                    dst_echo_delay = self.echo_delay[dst_dpid]
                    port_delays.append(src_lldp_delay - (dst_echo_delay + src_echo_delay) / 2)
            self.features.append([src_dpid, src_echo_delay, max(port_delays), min(port_delays), self.avg(port_delays)])
