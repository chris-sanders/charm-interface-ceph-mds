import json
import socket

from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charms.reactive import is_state
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log, service_name
from charmhelpers.contrib.network.ip import format_ipv6_addr

from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
    is_request_complete,
    send_request_if_needed,
)


class CephClient(RelationBase):
    scope = scopes.UNIT

    auto_accessors = ['fsid', 'auth']

    @hook('{requires:ceph-mds}-relation-{joined}')
    def joined(self):
        self.set_remote(key='mds-name', value=socket.gethostname())
        self.set_state('{relation_name}.connected')
        if not is_state('ceph-mds.custom.init'):
            log('Using default init', level='debug')
            self.initialize_mds(name=service_name())

    @hook('{requires:ceph-mds}-relation-{changed,departed}')
    def changed(self):
        data = {
            'mds-key': self.mds_key(),  # This is to support old relations that aren't host aware
            'mds-key-{}'.format(socket.gethostname()): self.mds_key(),
            'fsid': self.fsid(),
            'auth': self.auth(),
            'mon_hosts': self.mon_hosts()
        }
        if all(data.values()):
            self.set_state('{relation_name}.available')

        json_rq = self.get_local(key='broker_req')
        if json_rq:
            rq = CephBrokerRq()
            j = json.loads(json_rq)
            rq.ops = j['ops']
            log("changed broker_req: {}".format(rq.ops))

            if rq and is_request_complete(rq,
                                          relation=self.relation_name):
                log("Setting ceph-mds.pools.available")
                self.set_state('{relation_name}.pools.available')
            else:
                log("incomplete request. broker_req not found")

    @hook('{requires:ceph-mds}-relation-{broken}')
    def broken(self):
        self.remove_state('{relation_name}.available')
        self.remove_state('{relation_name}.connected')
        self.remove_state('{relation_name}.pools.available')
        self.remove_state('{relation_name}.initialized')
        self.remove_state('cephfs.configured')
        self.remove_state('ceph-mds.custom.init')
        self.set_local(key='broker_req', value=None)

    def mds_key(self):
        return self.get_remote('mds-key-{}'.format(socket.gethostname()))

    def initialize_mds(self, name, replicas=3, pool_type=None, weight=None,
                       config_flags=None):
        """
        Request pool setup and mds creation

        @param name: name of mds pools to create
        @param replicas: number of replicas for supporting pools
        """
        # json.dumps of the CephBrokerRq()
        json_rq = self.get_local(key='broker_req')

        if not json_rq:
            rq = CephBrokerRq()
            # Create data pool
            if pool_type == 'erasure':
                if config_flags.get('profile') != 'default':
                    rq.ops.append({
                        'op': 'create-erasure-profile',
                        'erasure-type': config_flags.get('erasure-type'),
                        'failure-domain': config_flags.get('failure-domain'),
                        'name': config_flags.get('profile'),
                        'k': config_flags.get('k'),
                        'm': config_flags.get('m'),
                        'l': config_flags.get('l'),
                    })

                rq.ops.append({
                    'op': 'create-pool',
                    'pool-type': pool_type,
                    'name': "{}_data".format(name),
                    'erasure-profile': config_flags.get('profile'),
                    'weight': weight,
                })
                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'allow_ec_overwrites',
                    'value': True,
                })
            else:
                rq.add_op_create_pool(name="{}_data".format(name),
                                      replica_count=replicas,
                                      weight=weight)

            if config_flags.get('compression-mode'):
                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_mode',
                    'value': config_flags.get('compression-mode'),
                })

                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_algorithm',
                    'value': config_flags.get('compression-algorithm'),
                })

                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_required_ratio',
                    'value': config_flags.get('compression-required-ratio'),
                })

            # Create metadata pool
            rq.add_op_create_pool(name="{}_metadata".format(name),
                                  replica_count=replicas,
                                  weight=weight)

            # Create CephFS
            rq.ops.append({
                'op': 'create-cephfs',
                'mds_name': name,
                'data_pool': "{}_data".format(name),
                'metadata_pool': "{}_metadata".format(name),
            })
            self.set_local(key='broker_req', value=rq.request)
            send_request_if_needed(rq, relation=self.relation_name)
        else:
            rq = CephBrokerRq()
            try:
                j = json.loads(json_rq)
                log("Json request: {}".format(json_rq))
                rq.ops = j['ops']
                send_request_if_needed(rq, relation=self.relation_name)
            except ValueError as err:
                log("Unable to decode broker_req: {}.  Error: {}".format(
                    json_rq, err))
        self.set_state('{relation_name}.initialized')

    def get_remote_all(self, key, default=None):
        """Return a list of all values presented by remote units for key"""
        # TODO: might be a nicer way todo this - written a while back!
        values = []
        for conversation in self.conversations():
            for relation_id in conversation.relation_ids:
                for unit in hookenv.related_units(relation_id):
                    value = hookenv.relation_get(key,
                                                 unit,
                                                 relation_id) or default
                    if value:
                        values.append(value)
        return list(set(values))

    def mon_hosts(self):
        """List of all monitor host public addresses"""
        hosts = []
        addrs = self.get_remote_all('ceph-public-address')
        for addr in addrs:
            hosts.append('{}:6789'.format(format_ipv6_addr(addr) or addr))
        hosts.sort()
        return hosts
