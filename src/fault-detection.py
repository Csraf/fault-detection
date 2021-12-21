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
from ryu.lib import dpid as dpid_lib
from ryu.lib import stplib
from ryu.app import simple_switch_13


class SimpleSwitch13(simple_switch_13):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'stplib': stplib.Stp}

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}  # {'dpid':{'dst_mac': 'port'}}
        self.stp = kwargs['stplib']
        self.network = nx.DiGraph()
        self.topology_api_app = self
        self.detection_cycle = 10
        self.datapaths = {}  # all switch set
        self.features = []
        self.echo_delay = {}  # {'dpid': 20ms}
        self.port_delay = {}
        self.switches = lookup_service_brick('switches')
        self.monitor_thread = hub.spawn(self._monitor)

        # Sample of stplib config.
        #  please refer to stplib.Stp.set_config() for details.
        config = {dpid_lib.str_to_dpid('0000000000000001'):
                      {'bridge': {'priority': 0x8000}},
                  dpid_lib.str_to_dpid('0000000000000002'):
                      {'bridge': {'priority': 0x9000}},
                  dpid_lib.str_to_dpid('0000000000000003'):
                      {'bridge': {'priority': 0xa000}}}
        self.stp.set_config(config)

    def _monitor(self):
        """
        execute the fault-detection algorithm for each 60s
        :return:
        """
        """ get the features in data plane"""
        while True:
            print("send echo request ")
            self.send_echo_request()
            hub.sleep(30)

            print("get the features in data plane")
            self.get_delay_features()
            print(self.features)

            print("executing the fault detection algorithm")
            fault_id = self.fault_detection()

            if fault_id != None:
                print("fault switch is : ", fault_id)
                print("fault detection recover stage")
                dp = self.datapaths[fault_id]

                if dp.id in self.mac_to_port:
                    print("data plane del the switch")
                    self.delete_flow(dp)
                    print("control plane del the switch")
                    del self.mac_to_port[dp.id]

            else:
                print("data plane running normally")
            hub.sleep(30)

    def fault_detection(self):
        res = {}
        cnt = 0
        fault_id = None
        for sw in self.features:
            dpid = sw[0]
            for i in range(1, len(self.features)):
                if sw[i] >= self.detection_cycle:
                    cnt += sw[i]
            res[dpid] = cnt
        mx = max(res.values())
        for k, v in res.items():
            if v == mx:
                fault_id = k
                break
        return fault_id

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        # construct flow_mod message and send it.
        inst = [ofp_parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                 actions)]
        mod = ofp_parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    def delete_flow(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        for dst in self.mac_to_port[datapath.id].keys():
            match = parser.OFPMatch(eth_dst=dst)
            mod = parser.OFPFlowMod(
                datapath, command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                priority=1, match=match)
            datapath.send_msg(mod)

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

    @set_ev_cls(stplib.EventTopologyChange, MAIN_DISPATCHER)
    def _topology_change_handler(self, ev):
        dp = ev.dp
        dpid_str = dpid_lib.dpid_to_str(dp.id)
        msg = 'Receive topology change event. Flush MAC table.'
        self.logger.debug("[dpid=%s] %s", dpid_str, msg)

        if dp.id in self.mac_to_port:
            self.delete_flow(dp)
            del self.mac_to_port[dp.id]

    @set_ev_cls(stplib.EventPortStateChange, MAIN_DISPATCHER)
    def _port_state_change_handler(self, ev):
        dpid_str = dpid_lib.dpid_to_str(ev.dp.id)
        of_state = {stplib.PORT_STATE_DISABLE: 'DISABLE',
                    stplib.PORT_STATE_BLOCK: 'BLOCK',
                    stplib.PORT_STATE_LISTEN: 'LISTEN',
                    stplib.PORT_STATE_LEARN: 'LEARN',
                    stplib.PORT_STATE_FORWARD: 'FORWARD'}
        self.logger.debug("[dpid=%s][port=%d] state=%s",
                          dpid_str, ev.port_no, of_state[ev.port_state])

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
        parser = datapath.ofproto_parser

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
            self.mac_to_port.setdefault(dpid, {})

            self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

            # learn a mac address to avoid FLOOD next time.
            self.mac_to_port[dpid][src] = in_port

            if dst in self.mac_to_port[dpid]:
                out_port = self.mac_to_port[dpid][dst]
            else:
                out_port = ofproto.OFPP_FLOOD

            actions = [parser.OFPActionOutput(out_port)]

            # install a flow to avoid packet_in next time
            if out_port != ofproto.OFPP_FLOOD:
                match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
                self.add_flow(datapath, 1, match, actions)

            data = None
            if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                data = msg.data

            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                      in_port=in_port, actions=actions, data=data)
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
            self.echo_delay[src_dpid] = self.detection_cycle
            port_delays = []
            for dst_dpid, edge in self.network[src_dpid].items():
                if 'delay' in edge.keys():
                    src_lldp_delay = edge['delay']
                    # reset port delay
                    edge['delay'] = self.detection_cycle
                    dst_echo_delay = self.echo_delay[dst_dpid]
                    port_delays.append(src_lldp_delay - (dst_echo_delay + src_echo_delay) / 2)
            self.features.append([src_dpid, src_echo_delay, max(port_delays), min(port_delays), self.avg(port_delays)])
        self.save_file(file_name='./features.txt', ls=self.features, mode='a')
