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
    """shortest path exchange"""

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ExampleShortestForwarding, self).__init__(*args, **kwargs)
        self.network = nx.DiGraph()
        self.topology_api_app = self
        self.detection_cycle = 10
        self.features = []
        self.paths = {}       # {src_mac: {dst_mac: []}}
        self.datapaths = {}   # {dpid: datapath}
        self.echo_delay = {}  # {dpid: time}
        self.port_delay = {}  # {dpid: {port: time}}
        self.switches = lookup_service_brick('switches')

        self.monitor_thread = hub.spawn(self._monitor)

    def _monitor(self):
        """
        execute the fault-detection algorithm for each 60s
        :return:
        """
        """ get the features in data plane"""
        while True:
            hub.sleep(10)  # wait mininet start
            print("send echo request ")
            self.send_echo_request()
            hub.sleep(20)  # wait pingall

            print("get the features in data plane")
            self.get_delay_features()
            print(self.features)

            print("executing the fault detection algorithm")
            # fault_id = self.fault_detection()
            # print("fault switch is : ", fault_id)

            # if fault_id != None:
            #     print("fault switch is : ", fault_id)
            #     print("fault detection recover stage")
            #     dp = self.datapaths[fault_id]
            #
            #     if dp.id in self.mac_to_port:
            #         print("data plane del the switch")
            #         self.delete_flow(dp)
            #         print("control plane del the switch")
            #         del self.mac_to_port[dp.id]
            #
            # else:
            #     print("data plane running normally")
            hub.sleep(30)

    # def fault_detection(self):
    #     res = {}
    #     cnt = 0
    #     fault_id = None
    #     for sw in self.features:
    #         dpid = sw[0]
    #         for i in range(1, len(self.features)):
    #             if sw[i] >= self.detection_cycle:
    #                 cnt += sw[i]
    #         res[dpid] = cnt
    #     mx = max(res.values())
    #     for k, v in res.items():
    #         if v == mx:
    #             fault_id = k
    #             break
    #     return fault_id

    def send_echo_request(self):
        """
        send echo request to all switches
        :return:
        """
        for datapath in self.datapaths.values():
            parser = datapath.ofproto_parser
            echo_req = parser.OFPEchoRequest(datapath, data=str("%.12f" % time.time()))
            datapath.send_msg(echo_req)
            print("send a echo successful")
            hub.sleep(0.5)

    @set_ev_cls(ofp_event.EventOFPEchoReply, [MAIN_DISPATCHER, CONFIG_DISPATCHER, HANDSHAKE_DISPATCHER])
    def echo_reply_handler(self, ev):
        """
        reply the echo pkt from all switches
        :param ev:
        :return:
        """
        end_timestamp = time.time()
        try:
            delay = end_timestamp - eval(ev.msg.data)
            self.echo_delay[ev.msg.datapath.id] = delay
            print("switch: ", ev.msg.datapath.id, " and controller echo delay : ", delay)
        except Exception as error:
            print("Exception in reply the echo pkt ")
            return

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
            record the state of switch by datapaths
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
        controller send the table-miss pkt when the switch be in config stage.
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
        print("network topology init finish in control plane")

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
            # TODO: a large pkts can't be abandoned in loop net because of the flood
            out_port = datapath.ofproto.OFPP_FLOOD

        return out_port

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """
        lldp pkt: record the time interval by the start and end timestamp.
        table-miss request: get the output port and send switch a flow table.
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

        try:  # lldp
            src_dpid, src_outport = LLDPPacket.lldp_parse(msg.data)
            dst_dpid = msg.datapath.id
            if self.switches is None:
                self.switches = lookup_service_brick("switches")

            for port in self.switches.ports.keys():
                if src_dpid == port.dpid and src_outport == port.port_no:
                    port_data = self.switches.ports[port]
                    start = port_data.timestamp
                    if start:
                        delay = time.time() - float(start)
                        # save delay to the network topology
                        self.network[src_dpid][dst_dpid]['delay'] = delay
                        # print("switch: ", src_dpid, " 'port ", port, " delay: ", delay)

        except Exception as error:  # table-miss
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
        """ get avg in list """
        sum = 0
        for item in ls:
            sum += item
        return sum / len(ls)

    @staticmethod
    def save_file(file_name=None, ls=None, mode='a'):
        with open(file_name, mode=mode) as f:
            for sub in ls:
                st = ''
                for i in range(len(sub)):
                    if i != len(sub) - 1:
                        st = (st + (str(sub[i]) + ','))
                    else:
                        st = st + (str(sub[i]))
                f.write(st + '\r')
            f.close()

    def get_delay_features(self):
        """
        get the delay in all port
        :return:
        """
        self.features = []
        for datapath in self.datapaths.values():
            src_dpid = datapath.id
            src_echo_delay = self.echo_delay[src_dpid]
            # reset echo delay
            # self.echo_delay[src_dpid] = self.detection_cycle
            port_delays = []
            for dst_dpid, edge in self.network[src_dpid].items():
                if 'delay' in edge.keys():
                    src_lldp_delay = edge['delay']
                    # reset port delay
                    # edge['delay'] = self.detection_cycle
                    dst_echo_delay = self.echo_delay[dst_dpid]
                    # port_delays.append(src_lldp_delay - (dst_echo_delay + src_echo_delay)/2)
                    port_delays.append(src_lldp_delay)
            self.features.append([src_dpid, src_echo_delay, max(port_delays), min(port_delays), self.avg(port_delays)])
        self.save_file(file_name='./features.txt', ls=self.features, mode='a')
