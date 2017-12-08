import json
import socket

from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log
from charmhelpers.contrib.network.ip import format_ipv6_addr

from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
    is_request_complete,
    send_request_if_needed,
)


class CephClient(RelationBase):
    scope = scopes.GLOBAL

    auto_accessors = ['mds_key', 'fsid', 'auth']

    @hook('{requires:ceph-mds}-relation-{joined}')
    def joined(self):
        self.set_remote(key='mds-name', value=socket.gethostname())
        self.set_state('{relation_name}.connected')

    @hook('{requires:ceph-mds}-relation-{changed,departed}')
    def changed(self):
        data = {
            'mds_key': self.mds_key(),
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

    def initialize_mds(self, name, **kwargs):
        """
        Request pool setup and mds creation

        @param name: name of mds pools to create
        @param pool-type: replicated or erasure pool
        @param replicas: number of replicas for replicated pools
        @param profile: The erasure profile to use or create
        @param weight: the percent_data charmhelpers uses to calculate pg num
        @param erasure-type: erasure algorithim for erasure pool
        @param failure-domain: erasure failure domain
        @param k: erasure data chunks
        @param m: erasure encoding chunks
        @param l: erasure locality
        """
        # json.dumps of the CephBrokerRq()
        json_rq = self.get_local(key='broker_req')

        if not json_rq:
            rq = CephBrokerRq()
            replicas = kwargs.get('replicas', 3)
            profile = kwargs.get('profile', 'default')
            # Create data pool
            if kwargs.get('pool-type') == 'erasure':
                if profile != 'default':
                    # Verify required inputs for cusotm profile
                    if kwargs.get('erasure-type') is None or\
                       kwargs.get('failure-domain') is None or\
                       kwargs.get('k') is None or\
                       kwargs.get('m') is None:
                        raise ValueError('Creating erasure profile requires'
                                         ' erasure-type, failure-domain, k, and'
                                         ' m')
                    rq.ops.append({
                        'op': 'create-erasure-profile',
                        'erasure-type': kwargs.get('erasure-type'),
                        'failure-domain': kwargs.get('failure-domain'),
                        'name': profile,
                        'k': kwargs.get('k'),
                        'm': kwargs.get('m'),
                        'l': kwargs.get('l'),
                    })
                rq.ops.append({
                    'op': 'create-pool',
                    'pool-type': kwargs.get('pool-type'),
                    'name': "{}_data".format(name),
                    'erasure-profile': profile,
                    'weight': kwargs.get('weight'),
                    'app-name': 'cephfs',
                })
                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'allow_ec_overwrites',
                    'value': True,
                })
            else:
                # Default to replicated pool
                rq.ops.append({
                    'op': 'create-pool',
                    'name': "{}_metadata".format(name),
                    'replicas': replicas,
                    'weight': kwargs.get('weight'),
                    'app-name': 'cephfs',
                })

            if kwargs.get('compression-mode'):
                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_mode',
                    'value': kwargs.get('compression-mode'),
                })

                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_algorithm',
                    'value': kwargs.get('compression-algorithm'),
                })

                rq.ops.append({
                    'op': 'set-pool-value',
                    'name': '{}_data'.format(name),
                    'key': 'compression_required_ratio',
                    'value': kwargs.get('compression-required-ratio'),
                })

            # Create metadata pool
            rq.ops.append({
                'op': 'create-pool',
                'name': "{}_metadata".format(name),
                'replicas': replicas,
                'weight': kwargs.get('weight'),
                'app-name': 'cephfs',
            })

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
