# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
import mock
from mock import patch

from airflow import configuration
from airflow.contrib.hooks.cassandra_hook import CassandraHook
from cassandra.cluster import Cluster
from cassandra.policies import (TokenAwarePolicy, RoundRobinPolicy,
    DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy)
from airflow import models
from airflow.utils import db


class CassandraHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='cassandra_test', conn_type='cassandra',
                host='host-1,host-2', port='9042', schema='test_keyspace',
                extra='{"load_balancing_policy":"TokenAwarePolicy"}'))

    def test_get_conn(self):
        with mock.patch.object(Cluster, "connect") as mock_connect, \
                mock.patch("socket.getaddrinfo", return_value=[]) as mock_getaddrinfo:
            mock_connect.return_value = 'session'
            hook = CassandraHook(cassandra_conn_id='cassandra_test')
            hook.get_conn()
            mock_getaddrinfo.assert_called()
            mock_connect.assert_called_once_with('test_keyspace')

            cluster = hook.get_cluster()
            self.assertEqual(cluster.contact_points, ['host-1', 'host-2'])
            self.assertEqual(cluster.port, 9042)
            self.assertTrue(isinstance(cluster.load_balancing_policy, TokenAwarePolicy))

    def test_get_policy(self):
        # test default settings for LB policies
        self._assert_get_policy('RoundRobinPolicy', {}, RoundRobinPolicy)
        self._assert_get_policy('DCAwareRoundRobinPolicy', {}, DCAwareRoundRobinPolicy)
        self._assert_get_policy('TokenAwarePolicy', {}, TokenAwarePolicy,
                                expected_child_policy_type=RoundRobinPolicy)

        # test custom settings for LB policies
        self._assert_get_policy('DCAwareRoundRobinPolicy',
                                {'local_dc': 'foo', 'used_hosts_per_remote_dc': '3'},
                                DCAwareRoundRobinPolicy)
        fake_addr_info =[['family', 'sockettype', 'proto', 'canonname',
                        ('2606:2800:220:1:248:1893:25c8:1946', 80, 0, 0)]]
        with patch('socket.getaddrinfo', return_value=fake_addr_info):
            self._assert_get_policy('WhiteListRoundRobinPolicy',
                                    {'hosts': ['host1', 'host2']},
                                    WhiteListRoundRobinPolicy)
        with patch('socket.getaddrinfo', return_value=fake_addr_info):
            self._assert_get_policy('TokenAwarePolicy',
                                    {'child_load_balancing_policy':
                                        'WhiteListRoundRobinPolicy',
                                     'child_load_balancing_policy_args':
                                        {'hosts': ['host1', 'host2', 'host3']}},
                                    TokenAwarePolicy,
                                    expected_child_policy_type=WhiteListRoundRobinPolicy)

        # invalid policy name should default to RoundRobinPolicy
        self._assert_get_policy('DoesNotExistPolicy', {}, RoundRobinPolicy)

        # invalid child policy name should default child policy to RoundRobinPolicy
        self._assert_get_policy('TokenAwarePolicy', {}, TokenAwarePolicy,
                                expected_child_policy_type=RoundRobinPolicy)
        self._assert_get_policy('TokenAwarePolicy',
                                {'child_load_balancing_policy': 'DoesNotExistPolicy'},
                                TokenAwarePolicy,
                                expected_child_policy_type=RoundRobinPolicy)

        # host not specified for WhiteListRoundRobinPolicy should throw exception
        self._assert_get_policy('WhiteListRoundRobinPolicy', {}, WhiteListRoundRobinPolicy,
                                should_throw=True)
        self._assert_get_policy('TokenAwarePolicy',
                                {'child_load_balancing_policy': 'WhiteListRoundRobinPolicy'},
                                TokenAwarePolicy,
                                expected_child_policy_type=RoundRobinPolicy,
                                should_throw=True)

    def _assert_get_policy(self, name, args, expected_policy_type,
                           expected_child_policy_type=None,
                           should_throw=False):
        thrown = False
        try:
            policy = CassandraHook.get_policy(name, args)
            self.assertTrue(isinstance(policy, expected_policy_type))
            if expected_child_policy_type:
                self.assertTrue(isinstance(policy._child_policy,
                                           expected_child_policy_type))
        except:
            thrown = True
        self.assertEqual(should_throw, thrown)


if __name__ == '__main__':
    unittest.main()
