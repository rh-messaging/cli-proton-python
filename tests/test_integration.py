#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Red Hat Inc.
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
""" cli-proton-python integration test suite """

from __future__ import print_function, absolute_import

import threading
import subprocess
import ast
import unittest
import random
import time
import sys
import proton

from cli_proton_python import sender, receiver, connector


# Client executor classes

class SenderReceiverTestCase(unittest.TestCase):
    """ Sender / Recevier wrapper test class """

    recv_messages = None

    def setUp(self):
        """ set up """
        pass

    def tearDown(self):
        """ tear down """
        self.run_receiver()

    @staticmethod
    def get_sender(opts):
        """ instantiates and return sender instance """
        return sender.Send(opts)

    @staticmethod
    def get_receiver(opts):
        """ instantiates and return receiver instance """
        return receiver.Recv(opts)

    @staticmethod
    def get_sender_opts():
        """ returns the default sender options """
        parser = sender.options.SenderOptions()
        opts, _ = parser.parse_args()
        opts.log_msgs = 'store'
        return opts

    @staticmethod
    def get_receiver_opts():
        """ returns the default receiver options """
        parser = sender.options.ReceiverOptions()
        opts, _ = parser.parse_args()
        opts.log_msgs = 'store'
        return opts

    def run_sender(self, in_opts=None):
        """ executes the sender with given or default options """
        opts = in_opts or self.get_sender_opts()
        send = self.get_sender(opts)
        container = proton.reactor.Container(send)
        container.run()
        return send.get_messages()

    def run_receiver(self, in_opts=None):
        """ executes the receiver with given or default options """
        opts = in_opts or self.get_receiver_opts()
        recv = self.get_receiver(opts)
        container = proton.reactor.Container(recv)
        container.run()
        return recv.get_messages()


class TxSenderReceiverTestCase(SenderReceiverTestCase):
    """ transactional Sender / Recevier wrapper test class """

    @staticmethod
    def get_sender(opts):
        return sender.TxSend(opts)

    @staticmethod
    def get_receiver(opts):
        return receiver.TxRecv(opts)

    def get_sender_opts(self):
        """ returns the default sender options """
        opts = super(TxSenderReceiverTestCase, self).get_sender_opts()
        opts.tx_size = 1
        return opts

    def get_receiver_opts(self):
        """ returns the default receiver options """
        opts = super(TxSenderReceiverTestCase, self).get_receiver_opts()
        opts.tx_size = 1
        return opts

class P2PTestCase(SenderReceiverTestCase):
    """ listener wrapper test class """

    def tearDown(self):
        pass

    def get_sender_opts(self):
        """ returns the default receiver options """
        opts = super(P2PTestCase, self).get_sender_opts()
        opts.broker_url = 'localhost:8888'
        return opts

    def get_receiver_opts(self):
        """ returns the default receiver options """
        opts = super(P2PTestCase, self).get_receiver_opts()
        opts.broker_url = 'localhost:8888'
        opts.recv_listen = True
        return opts

    def run_receiver(self, in_opts=None):
        """ executes the receiver with given or default options """
        self.recv_messages = super(P2PTestCase, self).run_receiver(in_opts)


class ConnectorTestCase(unittest.TestCase):
    """ Connector wrapper test class """

    @staticmethod
    def get_connector_opts():
        """ returns the default connector options """
        parser = sender.options.ConnectorOptions()
        opts, _ = parser.parse_args()
        opts.log_msgs = 'store'
        opts.obj_ctrl = 'CESR'
        return opts

    def run_connector(self, in_opts=None):
        """ executes the connector with given or default options """
        opts = in_opts or self.get_connector_opts()
        conn = connector.Connector(opts)
        container = proton.reactor.Container(conn)
        container.run()
        return conn.get_messages()


class CommandLineTestCase(unittest.TestCase):
    """ command line clients wrapper test class """

    @staticmethod
    def run_client(cli_path, opts, wait=True):
        """
        executes the connector with given or default options

        return client stdout or client instance when running on background
        """
        cli = subprocess.Popen([cli_path] + (opts or []), stderr=subprocess.STDOUT,
                               stdout=subprocess.PIPE, universal_newlines=True)
        if not wait:
            return cli
        cli.wait()
        stdout = [l.strip() for l in cli.stdout]
        cli.stdout.close()
        return stdout

    def run_sender(self, in_opts=None):
        """ executes the connector with given or default options """
        return self.run_client('../cli_proton_python/sender.py', in_opts)

    def run_receiver(self, in_opts=None, in_wait=True):
        """ executes the connector with given or default options """
        return self.run_client('../cli_proton_python/receiver.py', in_opts, in_wait)

    def run_connector(self, in_opts=None):
        """ executes the connector with given or default options """
        return self.run_client('../cli_proton_python/connector.py', in_opts)

# Tests


class MessageDeliveryTests(SenderReceiverTestCase):
    """ message delivery test group """

    def test_send_receive(self):
        """ tests basic send and receive of a message """
        sent_messages = self.run_sender()
        recv_messages = self.run_receiver()
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_send_receive_hundred(self):
        """ tests basic send and receive of a message """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.count = 100
        recv_opts.count = 100
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertEqual(len(sent_messages), send_opts.count)
        self.assertEqual(len(recv_messages), send_opts.count)


class MessageTypeTests(SenderReceiverTestCase):
    """ message type test group """

    def test_msg_type(self):
        """ tests type of sent and received message """
        sent_messages = self.run_sender()
        recv_messages = self.run_receiver()
        self.assertIsInstance(sent_messages[0], proton.Message)
        self.assertIsInstance(recv_messages[0], proton.Message)


class MessageOptionsTests(SenderReceiverTestCase):
    """ message fields test group """

    def test_msg_correlation_id(self):
        """ tests message corelation field """
        send_opts = self.get_sender_opts()
        send_opts.msg_correlation_id = 'correlation id'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].correlation_id, send_opts.msg_correlation_id)
        self.assertEqual(sent_messages[0].correlation_id, recv_messages[0].correlation_id)

    def test_msg_durable(self):
        """ tests message durable field """
        send_opts = self.get_sender_opts()
        send_opts.msg_durable = 'True'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].durable, True)
        self.assertEqual(sent_messages[0].durable, recv_messages[0].durable)

    def test_msg_id(self):
        """ tests message id field """
        send_opts = self.get_sender_opts()
        send_opts.msg_id = 'testId'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].id, send_opts.msg_id)
        self.assertEqual(sent_messages[0].id, recv_messages[0].id)

    def test_msg_user_id(self):
        """ tests message user id field """
        send_opts = self.get_sender_opts()
        send_opts.msg_user_id = 'anonymous'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].user_id.decode(), send_opts.msg_user_id)
        self.assertEqual(sent_messages[0].user_id.decode(), recv_messages[0].user_id.decode())

    def test_msg_group_id(self):
        """ tests message group id field """
        send_opts = self.get_sender_opts()
        send_opts.msg_group_id = 'anonymous'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].group_id, send_opts.msg_group_id)
        self.assertEqual(sent_messages[0].group_id, recv_messages[0].group_id)

    def test_test_msg_group_seq(self):
        """ tests message group seq fields"""
        send_opts = self.get_sender_opts()
        send_opts.msg_group_seq = 1
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].group_sequence, send_opts.msg_group_seq)
        self.assertEqual(sent_messages[0].group_sequence, recv_messages[0].group_sequence)

    def test_msg_priority(self):
        """ tests message priority field """
        send_opts = self.get_sender_opts()
        send_opts.msg_priority = 1
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].priority, send_opts.msg_priority)
        self.assertEqual(sent_messages[0].priority, recv_messages[0].priority)

    def test_msg_ttl(self):
        """ tests message time to live field """
        send_opts = self.get_sender_opts()
        send_opts = self.get_sender_opts()
        send_opts.msg_ttl = 500
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].ttl, send_opts.msg_ttl/1000.0)
        self.assertLessEqual(recv_messages[0].ttl, send_opts.msg_ttl/1000.0)

    def test_msg_address(self):
        """ tests message address field """
        send_opts = self.get_sender_opts()
        send_opts.msg_address = 'examples'
        send_opts.broker_url = '127.0.0.1:5672'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].address, send_opts.msg_address)
        self.assertEqual(sent_messages[0].address, recv_messages[0].address)

    def test_msg_reply_to(self):
        """ tests message reply to address field """
        send_opts = self.get_sender_opts()
        send_opts.msg_reply_to = 'examples'
        recv_opts = self.get_receiver_opts()
        recv_opts.process_reply_to = True
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertEqual(sent_messages[0].reply_to, send_opts.msg_reply_to)
        self.assertEqual(sent_messages[0].reply_to, recv_messages[0].reply_to)

    def test_msg_properties(self):
        """ tests message properties """
        send_opts = self.get_sender_opts()
        send_opts.msg_properties = {'test=property'}
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].properties, {'test':'property'})
        self.assertEqual(sent_messages[0].properties, recv_messages[0].properties)


class MessageContentTests(SenderReceiverTestCase):
    """ message content test group """

    def test_msg_content_string(self):
        """ tests text message """
        send_opts = self.get_sender_opts()
        send_opts.msg_content = 'text message'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].body, send_opts.msg_content)
        self.assertEqual(sent_messages[0].body, recv_messages[0].body)
        self.assertEqual(sent_messages[0].content_type, 'text/plain')
        self.assertEqual(sent_messages[0].content_type, recv_messages[0].content_type)

    def test_msg_content_string_chars(self):
        """ tests text message """
        send_opts = self.get_sender_opts()
        send_opts.msg_content = r'+ěščřžýáíéé=)ů§.-!@#$%^&*()_[]\;/.,\'`~'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].body, send_opts.msg_content)
        self.assertEqual(sent_messages[0].body, recv_messages[0].body)
        self.assertEqual(sent_messages[0].content_type, 'text/plain')
        self.assertEqual(sent_messages[0].content_type, recv_messages[0].content_type)

    def test_msg_content_list(self):
        """ tests list message """
        send_opts = self.get_sender_opts()
        send_opts.msg_list_items = ['list message']
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertIsInstance(sent_messages[0].body, list)
        self.assertIsInstance(recv_messages[0].body, list)
        self.assertEqual(sent_messages[0].content_type, 'amqp/list')
        self.assertEqual(recv_messages[0].content_type, 'amqp/list')

    def test_msg_content_map(self):
        """ tests map message """
        send_opts = self.get_sender_opts()
        send_opts.msg_map_items = ['map=message']
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertIsInstance(sent_messages[0].body, dict)
        self.assertIsInstance(recv_messages[0].body, dict)
        self.assertEqual(sent_messages[0].content_type, 'amqp/map')
        self.assertEqual(recv_messages[0].content_type, 'amqp/map')

    def test_msg_content_numbering(self):
        """ tests message numbering """
        send_opts = self.get_sender_opts()
        send_opts.msg_content = 'message %d'
        send_opts.count = random.randint(2, 10)
        self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(len(recv_messages), send_opts.count)
        self.assertEqual(recv_messages[0].body, 'message 0')
        self.assertEqual(recv_messages[(send_opts.count)-1].body, "message %s" %(send_opts.count-1))


@unittest.skip("test not implemented yet")
class MessageValuesRetypeTests(SenderReceiverTestCase):
    """ retype message fields test group """
    pass


@unittest.skip("test not implemented yet")
class AuthenticationTests(SenderReceiverTestCase):
    """ clients authentication test group """

    def test_client_authentication(self):
        """ tests client authentication """
        pass


@unittest.skip("test class not implemented yet")
class ReactorOptionsTests(SenderReceiverTestCase):
    """ reactor options test group """
    pass


class ConnectionOptionsTests(SenderReceiverTestCase):
    """ connection options test group """

    def test_auth_mechs_anonymous(self):
        """ tests allowed authentication mechanisms: anonymous """
        send_opts = self.get_sender_opts()
        send_opts.conn_allowed_mechs = 'ANONYMOUS'
        recv_opts = self.get_receiver_opts()
        recv_opts.conn_allowed_mechs = 'ANONYMOUS'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_read_config_file_send_receive_opts(self):
        """ tests connection from configuration file """
        send_opts = self.get_sender_opts()
        send_opts.conn_use_config_file = True
        recv_opts = self.get_receiver_opts()
        recv_opts.conn_use_config_file = True
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

@unittest.skip("test class not implemented yet")
class LinkOptionsTests(SenderReceiverTestCase):
    """ link options test group """
    pass


class LoggingOptionsTests(CommandLineTestCase):
    """ logging options test group """

    def test_messages_logging_dict(self):
        """ tests messages logging option """
        sent_messages = self.run_sender(['--log-msgs', 'dict'])
        recv_messages = self.run_receiver(['--log-msgs', 'dict'])
        self.assertTrue(isinstance(ast.literal_eval(sent_messages[0]), dict))
        self.assertTrue(isinstance(ast.literal_eval(recv_messages[0]), dict))

    def test_messages_logging_body(self):
        """ tests messages logging option """
        sent_messages = self.run_sender(['--log-msgs', 'body'])
        recv_messages = self.run_receiver(['--log-msgs', 'body'])
        self.assertEqual(sent_messages[0], 'None')
        self.assertEqual(recv_messages[0], 'None')

    def test_messages_logging_upstream(self):
        """ tests messages logging option """
        sent_messages = self.run_sender(['--log-msgs', 'upstream'])
        recv_messages = self.run_receiver(['--log-msgs', 'upstream'])
        self.assertTrue(sent_messages[0].startswith('Message'))
        self.assertTrue(recv_messages[0].startswith('Message'))

    def test_messages_logging_none(self):
        """ tests messages logging option """
        sent_messages = self.run_sender(['--log-msgs', 'none'])
        recv_messages = self.run_receiver(['--log-msgs', 'none'])
        self.assertTrue(len(sent_messages) == len(recv_messages) == 0)

    def test_messages_logging_interop(self):
        """ tests messages logging option """
        sent_messages = self.run_sender(['--log-msgs', 'interop'])
        recv_messages = self.run_receiver(['--log-msgs', 'interop'])
        self.assertTrue(isinstance(ast.literal_eval(sent_messages[0]), dict))
        self.assertTrue(isinstance(ast.literal_eval(recv_messages[0]), dict))

    @unittest.skip("test not implemented yet")
    def test_statistics_logging(self):
        """ tests statistics logging option """
        pass

    @unittest.skip("test not implemented yet")
    def test_library_logging(self):
        """ tests proton logging option """


class ControlOptionsTests(SenderReceiverTestCase):
    """ control options test group """

    def test_broker_url(self):
        """ tests broker url option """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.broker_url = '127.0.0.1:5672/examples'
        recv_opts.broker_url = '127.0.0.1:5672/examples'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_messages_count(self):
        """ tests meesage count option """
        send_opts = self.get_sender_opts()
        send_opts.count = random.randint(1, 10)
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertTrue(len(sent_messages) == len(recv_messages) == send_opts.count)

    def test_duration_sender(self):
        """ tests sender's duration option """
        send_opts = self.get_sender_opts()
        send_opts.duration = 1
        send_opts.count = 2
        tstamp = time.time()
        self.run_sender(send_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 0.9)
        self.run_receiver()

    def test_timeout_receiver(self):
        """ tests recever's time-out option """
        recv_opts = self.get_receiver_opts()
        recv_opts.timeout = 1
        tstamp = time.time()
        self.run_receiver(recv_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 0.9)

    def test_timeout_sender(self):
        """ tests sender's time-out option """
        send_opts = self.get_sender_opts()
        send_opts.broker_url = '127.0.0.1:5673/examples'
        send_opts.timeout = 1
        tstamp = time.time()
        self.run_sender(send_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 0.9)

    @unittest.skip("known issue #17")
    def test_duration_timeout_receiver(self):
        """ tests combining receiver's duration and timeout option """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.count = 2
        recv_opts.count = 2
        recv_opts.duration = 3
        recv_opts.timeout = 1
        self.run_sender(send_opts)
        tstamp = time.time()
        recv_messages = self.run_receiver(recv_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 0.9)
        self.assertEqual(len(recv_messages), 1)

    def test_duration_receiver(self):
        """ tests receiver's duration option """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.count = 2
        recv_opts.count = 2
        recv_opts.duration = 1
        self.run_sender(send_opts)
        tstamp = time.time()
        self.run_receiver(recv_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 0.9)

    @unittest.skip("test not implemented yet")
    def test_duration_mode(self):
        """ tests duration mode option """
        pass

    @unittest.skip("test not implemented yet")
    def test_capacity(self):
        """ tests capacity option """
        pass

    @unittest.skip("test not implemented yet")
    def test_dynamic(self):
        """ tests dynamic flag option """
        pass

    def test_close_sleep(self):
        """ tests close sleep option """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.close_sleep = 1
        recv_opts.close_sleep = 1
        tstamp = time.time()
        self.run_sender(send_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 1)
        tstamp = time.time()
        self.run_receiver(recv_opts)
        self.assertTrue(1.1 > time.time() - tstamp > 1)

    @unittest.skip("test not implemented yet")
    def test_sync_mode(self):
        """ tests synchronization mode option """
        pass


class ReceiverOptionsTests(SenderReceiverTestCase):
    """ receiver options test group """

    def test_message_reply_to(self):
        """ tests message reply to address option """
        send_opts = self.get_sender_opts()
        send_opts.msg_reply_to = 'examples'
        recv_opts = self.get_receiver_opts()
        recv_opts.process_reply_to = True
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        reply_to_messages = self.run_receiver()
        self.assertEqual(sent_messages[0].reply_to, send_opts.msg_reply_to)
        self.assertEqual(sent_messages[0].reply_to, recv_messages[0].reply_to)
        self.assertEqual(len(reply_to_messages), 1)

    @unittest.skip("test not implemented yet")
    def test_message_action(self):
        """ tests message action option """
        pass

    @unittest.skip("test not implemented yet")
    def test_message_batch_action(self):
        """ tests message batch action option """
        pass

    def test_message_selector(self):
        """ tests message selector option """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.msg_properties = {u'test=selector-non-match'}
        self.run_sender(send_opts)
        send_opts.msg_properties = {u'test=selector-match'}
        self.run_sender(send_opts)
        recv_opts.recv_selector = "test = 'selector-match'"
        recv_messages = self.run_receiver(recv_opts)
        self.assertEqual(len(recv_messages), 1)
        self.assertEqual(recv_messages[0].properties, {'test': 'selector-match'})

    def test_browse(self):
        """ tests browse option """
        self.run_sender()
        recv_opts = self.get_receiver_opts()
        recv_opts.recv_browse = True
        recv_messages = self.run_receiver(recv_opts)
        self.assertEqual(len(recv_messages), 1)
        recv_messages = self.run_receiver()
        self.assertEqual(len(recv_messages), 1)

    def test_consume(self):
        """ tests consume message option (default) """
        self.run_sender()
        recv_opts = self.get_receiver_opts()
        recv_opts.recv_consume = True
        recv_messages = self.run_receiver(recv_opts)
        self.assertEqual(len(recv_messages), 1)
        recv_messages = self.run_receiver()
        self.assertEqual(len(recv_messages), 0)

    @unittest.skip("test not implemented yet")
    def test_message_filter(self):
        """ tests message filter option """
        pass

    @unittest.skip("test not implemented yet")
    def test_listener(self):
        """ point-to-point tests covered in P2PTests

        .. :seealso:: P2PTests
        """
        pass


# Transactional tests

class TxControlOptionsTests(ControlOptionsTests, TxSenderReceiverTestCase):
    """ transactional options test group """

    @unittest.skip("known issue#18")
    def test_timeout_receiver(self):
        pass

class TxMessageDeliveryTests(MessageDeliveryTests, TxSenderReceiverTestCase):
    """ transactional test group """

class TxMessageTypeTests(MessageTypeTests, TxSenderReceiverTestCase):
    ''' transactional message type test group '''

class TxMessageOptionsTests(MessageOptionsTests, TxSenderReceiverTestCase):
    ''' transactional message fields test group '''

    @unittest.skip("known issue#19")
    def test_msg_reply_to(self):
        """ skipped in transactional mode """

    @unittest.skip("currently disabled due to ARTEMIS-1535")
    def test_msg_durable(self):
        """ skipped in transactional mode """

class TxMessageContentTests(MessageContentTests, TxSenderReceiverTestCase):
    ''' transactional message content test group '''

class TxReceiverOptionsTests(ReceiverOptionsTests, TxSenderReceiverTestCase):
    """ transactional receiver options test group """

    @unittest.skip("known issue#19")
    def test_message_reply_to(self):
        """ skipped in transactional mode """

class TxLoggingOptionsTests(LoggingOptionsTests, TxSenderReceiverTestCase):
    """ transactional logging options test group """

class TxConnectionOptionsTests(ConnectionOptionsTests, TxSenderReceiverTestCase):
    """ transactional connection options test group """

class TransactionOptionsTests(TxSenderReceiverTestCase):
    """ transactional options test group """

    transactional = True

    def test_sender_action_commit(self):
        """ tests sender transaction commit """
        send_opts = self.get_sender_opts()
        send_opts.tx_action = 'commit'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_sender_action_rollback(self):
        """ tests sender transaction rolback """
        send_opts = self.get_sender_opts()
        send_opts.tx_action = 'rollback'
        self.run_sender(send_opts)
        recv_messages = self.run_receiver()
        self.assertEqual(len(recv_messages), 0)

    def test_action_commit(self):
        """ tests sender/receiver transaction commit """
        send_opts = self.get_sender_opts()
        recv_opts = self.get_receiver_opts()
        send_opts.tx_action = 'commit'
        recv_opts.tx_action = 'commit'
        sent_messages = self.run_sender(send_opts)
        recv_messages = self.run_receiver(recv_opts)
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)


# Connector tests

@unittest.skip("test class not implemented yet")
class ConnectorTests(ConnectorTestCase):
    """ connector client test group """

    def test_connect(self):
        """ test connect using connector client """
        conn_opts = self.get_connector_opts()
        self.run_connector(conn_opts)


class CommandLineTests(CommandLineTestCase):
    """ command line test group """

    def test_send_receive(self):
        """ basic send receive test """
        sent_messages = self.run_sender(['--log-msgs', 'dict'])
        recv_messages = self.run_receiver(['--log-msgs', 'dict'])
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_subscribe(self):
        """ basic subscription test """
        recv = self.run_receiver(['--log-msgs', 'dict', '-c', '1', '--timeout', '1'], False)
        sent_messages = self.run_sender(['--log-msgs', 'dict'])
        recv.wait()
        recv_messages = [l.strip() for l in recv.stdout]
        recv.stdout.close()
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)
    
    def test_read_config_file_send_receive_cli(self):
        """ basic send receive test with connection configuration file """
        sent_messages = self.run_sender(['--log-msgs', 'dict', '--conn-use-config-file'])
        recv_messages = self.run_receiver(['--log-msgs', 'dict', '--conn-use-config-file'])
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_conn_urls_send_receive_cli(self):
        """ basic send receive test with connection urls """
        sent_messages = self.run_sender([
            '--broker-url', '127.0.0.1:5671/examples',
            '--log-msgs', 'dict',
            '--conn-urls', '127.0.0.1:5670,127.0.0.1:5672',
        ])
        recv_messages = self.run_receiver([
            '--broker-url', '127.0.0.1:5671/examples',
            '--log-msgs', 'dict',
            '--conn-urls', '127.0.0.1:5670,127.0.0.1:5672',
        ])
        sent_messages = [m for m in sent_messages if m.startswith('{')]
        recv_messages = [m for m in recv_messages if m.startswith('{')]
        self.assertTrue(len(sent_messages) == len(recv_messages) == 1)

    def test_send_receive_on_release(self):
        """
        tests basic send and receive of a message using 10 concurrent receivers
        and enforces usage of '--on-release retry'
        :return:
        """
        # Total number of messages expected to be exchanged
        TOTAL_MSGS = 100
        # Number of concurrent receivers accepting messages
        RECV_INSTANCES = 10
        RECV_COUNT = TOTAL_MSGS/RECV_INSTANCES
        receivers = list()
        receiver_args = ['--action', 'release', '-b', '0.0.0.0:5673/examples',
                         '--log-msgs', 'dict', '-c', "%s" % int(RECV_COUNT),
                         '--timeout', '30']

        # running one single receiver that will release all messages
        recv_release = self.run_receiver(receiver_args, in_wait=False)

        # multiple receivers that will accept all messages
        for _ in range(RECV_INSTANCES):
            recv = self.run_receiver(receiver_args[2:], in_wait=False)
            receivers.append(recv)

        # running sender and retrieving amount of messages sent
        sent = self.run_sender(['-b', '0.0.0.0:5673/examples', '--log-msgs', 'dict',
                                '-c', "%s" % TOTAL_MSGS, '--timeout', '30',
                                '--on-release', 'retry'])
        sent_msgs = len(sent)

        # counting received messages (accepted)
        received_msgs = 0
        for recv in receivers:
            recv.wait()
            received_msgs += len(recv.stdout.readlines())
            recv.stdout.close()

        # waiting on recv_release to complete
        recv_release.wait()
        released_msgs = len(recv_release.stdout.readlines())
        recv_release.stdout.close()

        # sender must have sent the total amount msgs plus number of released msgs
        self.assertGreaterEqual(sent_msgs, TOTAL_MSGS + RECV_COUNT)
        self.assertEqual(received_msgs, TOTAL_MSGS)
        self.assertEqual(released_msgs, RECV_COUNT)


# Peer-to-peer tests

class P2PTests(P2PTestCase):
    """ point-to-point test group """

    def test_p2p_snd_rcv_threading(self):
        """ tests point-to-point delivery """
        recv_opts = self.get_receiver_opts()
        recv_opts.count = 1
        recv_thread = threading.Thread(target=self.run_receiver, args=[recv_opts])
        recv_thread.start()
        send_opts = self.get_sender_opts()
        send_opts.conn_allowed_mechs = 'ANONYMOUS'
        sent_messages = self.run_sender(send_opts)
        recv_thread.join()
        self.assertTrue(len(sent_messages) == len(self.recv_messages) == 1)

    def test_p2p_snd_rcv_subprocess(self):
        """ tests point-to-point delivery """
        rcv = subprocess.Popen(['../cli_proton_python/receiver.py', '-b', 'localhost:8888',
                                '-c', '1', '--recv-listen', '--log-msgs', 'dict'],
                               # TODO uncomment when the following issue is fixed:
                               #   https://issues.jboss.org/browse/ENTMQCL-1364
                               # stderr=subprocess.STDOUT, 
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
        time.sleep(0.1)
        snd = subprocess.Popen(['../cli_proton_python/sender.py', '-b', 'localhost:8888',
                                '--log-msgs', 'dict', '--conn-allowed-mechs', 'ANONYMOUS'],
                               stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                               universal_newlines=True)
        snd.wait()
        rcv.wait()
        sent_message = [l.strip() for l in snd.stdout]
        recv_message = [l.strip() for l in rcv.stdout]
        snd.stdout.close()
        rcv.stdout.close()
        self.assertTrue(isinstance(ast.literal_eval(sent_message[0]), dict))
        self.assertEqual(sent_message, recv_message)

    def test_p2p_snd_rcv_subprocess_sasl_enabled(self):
        """ tests point-to-point delivery with enabled sasl"""
        rcv = subprocess.Popen(['../cli_proton_python/receiver.py', '-b', 'localhost:8888',
                                '-c', '1', '--recv-listen', '--log-msgs', 'dict',
                                '--conn-sasl-enabled', 'true'],
                               # TODO uncomment when the following issue is fixed:
                               #   https://issues.jboss.org/browse/ENTMQCL-1364
                               # stderr=subprocess.STDOUT, 
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
        time.sleep(0.1)
        snd = subprocess.Popen(['../cli_proton_python/sender.py', '-b', 'localhost:8888',
                                '--log-msgs', 'dict', '--conn-allowed-mechs', 'ANONYMOUS',
                                '--conn-sasl-enabled', 'true'],
                               stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                               universal_newlines=True)
        snd.wait()
        rcv.wait()
        sent_message = [l.strip() for l in snd.stdout]
        recv_message = [l.strip() for l in rcv.stdout]
        snd.stdout.close()
        rcv.stdout.close()
        self.assertTrue(isinstance(ast.literal_eval(sent_message[0]), dict))
        self.assertEqual(sent_message, recv_message)

    def test_p2p_snd_rcv_subprocess_sasl_disabled(self):
        """ tests point-to-point delivery with disabled sasl"""
        rcv = subprocess.Popen(['../cli_proton_python/receiver.py', '-b', 'localhost:8888',
                                '-c', '1', '--recv-listen', '--log-msgs', 'dict',
                                '--conn-sasl-enabled', 'false'],
                               # TODO uncomment when the following issue is fixed:
                               #   https://issues.jboss.org/browse/ENTMQCL-1364
                               # stderr=subprocess.STDOUT, 
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
        time.sleep(0.1)
        snd = subprocess.Popen(['../cli_proton_python/sender.py', '-b', 'localhost:8888',
                                '--log-msgs', 'dict', '--conn-allowed-mechs', 'ANONYMOUS',
                                '--conn-sasl-enabled', 'false'],
                               stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                               universal_newlines=True)
        snd.wait()
        rcv.wait()
        sent_message = [l.strip() for l in snd.stdout]
        recv_message = [l.strip() for l in rcv.stdout]
        snd.stdout.close()
        rcv.stdout.close()
        self.assertTrue(isinstance(ast.literal_eval(sent_message[0]), dict))
        self.assertEqual(sent_message, recv_message)


if __name__ == '__main__':
    TRN = unittest.main(module=__name__, exit=False, verbosity=2)
    sys.exit(not TRN.result.wasSuccessful())
