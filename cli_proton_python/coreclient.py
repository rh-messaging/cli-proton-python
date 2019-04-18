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

""" Proton reactive API python core client module """

from __future__ import absolute_import, print_function, division

import time

import proton
import proton.reactor
import proton.handlers

from cli_proton_python import utils


class CoreClient(proton.handlers.MessagingHandler):
    """ Proton reactive API python core client

    implements various support methods for sender, recevier and connector
    """

    def __init__(self, opts, reactor_opts=None):
        """
        CoreClient constructor

        :param opts: core client options
        :type opts: optparse.Values instance
        :param reactor_opts: reactor options
        :type reactor_opts: dict
        """
        reactor_opts = reactor_opts or {}
        super(CoreClient, self).__init__(**reactor_opts)
        self.url = proton.Url(opts.broker_url)
        self.msg_total_cnt = None
        self.start_tm = time.time()
        self.opts = opts
        if getattr(opts, 'sync_mode', None) or getattr(opts, 'capacity', None):
            raise NotImplementedError("Options not implemented yet: 'sync_mode', 'capacity'")
        self.auto_settle = reactor_opts.get('auto_settle', True)
        self.timeout = None
        self.next_task = None
        self.tearing_down = False
        self.msgs = []

    def parse_connection_options(self):
        """
        Prepare options passed to container connect

        :return: connection options
        :rtype: dict
        """

        conn_opts = {}
        if self.opts.conn_urls is not None:
            if not isinstance(self.opts.conn_urls, (str, list)):
                raise ValueError('Invalid conn-urls value, expected string or list: %s'
                                 % self.opts.conn_urls)
            if isinstance(self.opts.conn_urls, str):
                conn_opts['urls'] = self.opts.conn_urls.split(',')
            else:
                conn_opts['urls'] = self.opts.conn_urls
        if self.opts.conn_reconnect == 'false':
            conn_opts['reconnect'] = False
        elif (self.opts.conn_reconnect_interval
              or self.opts.conn_reconnect_limit
              or self.opts.conn_reconnect_timeout):
            conn_opts['reconnect'] = CustomBackoff(self.opts.conn_reconnect_interval,
                                                   self.opts.conn_reconnect_limit or 99,
                                                   self.opts.conn_reconnect_timeout)
        if self.opts.conn_heartbeat is not None:
            if not isinstance(self.opts.conn_heartbeat, int):
                raise ValueError('Invalid conn-heartbeat value, expected number: %s'
                                 % self.opts.conn_heartbeat)
            conn_opts['heartbeat'] = self.opts.conn_heartbeat
        if self.opts.conn_handler is not None:
            raise NotImplementedError("Option not implemented yet: 'conn_handler'")
        if self.opts.conn_max_frame_size is not None:
            conn_opts['max_frame_size'] = self.opts.conn_max_frame_size
        if self.opts.conn_sasl_enabled == 'false':
            conn_opts['sasl_enabled'] = False
        if self.opts.conn_allowed_mechs is not None:
            conn_opts['allowed_mechs'] = self.opts.conn_allowed_mechs
        return conn_opts

    def parse_link_options(self):
        """
        Prepare link options passed

        :return: link options
        :rtype: list
        """
        link_opts = []
        if self.opts.link_at_least_once:
            link_opts.append(proton.reactor.AtLeastOnce())
        if self.opts.link_at_most_once:
            link_opts.append(proton.reactor.AtMostOnce())
        return link_opts

    def set_up_ssl_server_auth(self, event):
        """ set-up SSLDomain for the server verification

        | VERIFY_PEER: Require peer to provide a valid identifying certificate
        | VERIFY_PEER_NAME: Require valid certificate and matching name

        :param event: reactor event
        :type event: proton.Event
        """
        if not self.opts.conn_ssl_trust_store:
            raise ValueError('trust store path needs to be given: %s'
                             % self.opts.conn_ssl_trust_store)
        event.container.ssl.client.set_trusted_ca_db(self.opts.conn_ssl_trust_store)
        if self.opts.conn_ssl_verify_peer_name:
            event.container.ssl.client.set_peer_authentication(proton.SSLDomain.VERIFY_PEER_NAME)
        elif self.opts.conn_ssl_verify_peer:
            event.container.ssl.client.set_peer_authentication(proton.SSLDomain.VERIFY_PEER)

    def set_up_ssl_client_auth(self, event):
        """
        sets-up SSLDomain for the client authentication

        :param event: reactor event
        :type event: proton.Event
        """
        if not self.opts.conn_ssl_private_key:
            raise ValueError('client private key path needs to be given: %s'
                             % self.opts.conn_ssl_private_key)
        event.container.ssl.client.set_credentials(self.opts.conn_ssl_certificate,
                                                   self.opts.conn_ssl_private_key,
                                                   self.opts.conn_ssl_password)

    def set_up_ssl(self, event):
        """
        sets-up SSLDomain

        :param event: reactor event
        :type event: proton.Event
        """
        if (self.opts.conn_ssl_trust_store
                or self.opts.conn_ssl_verify_peer_name
                or self.opts.conn_ssl_verify_peer):
            self.set_up_ssl_server_auth(event)
        if self.opts.conn_ssl_certificate:
            self.set_up_ssl_client_auth(event)

    def tear_down(self, event, settled=False):
        """
        tears down and closes the connection

        :param event: reactor event
        :type event: proton.Event
        :param settled: indicates whether all messages has been explicitly settled
        :type settled: bool
        """
        if self.auto_settle or settled:
            if self.next_task is not None:
                self.next_task.cancel()
            if self.timeout is not None:
                self.timeout.cancel()
            if self.opts.log_stats == 'endpoints':
                utils.dump_event(event)
            self.tearing_down = True
            time.sleep(self.opts.close_sleep)
            if event.receiver:
                event.receiver.close()
            if event.sender:
                event.sender.close()
            if event.connection:
                event.connection.close()

    def print_message(self, message):
        '''
        prints or store a message

        utils.print_message wrapper

        :param msg: message
        :type msg: proton.Message
        :param msg_format: pre-defined message format
        :type msg_format: str
        '''
        if self.opts.log_msgs == 'store':
            self.msgs.append(message)
        else:
            utils.print_message(message, self.opts.log_msgs)

    def get_messages(self):
        ''' returns list of stored messages '''
        return self.msgs

    def clear_messages(self):
        ''' clears list of stored messages '''
        self.msgs = []

    def set_delay_before(self):
        """
        returns delay duration before sending a message (in seconds)

        :return: time to wait before message is sent/received
        :rtype: float
        """
        if self.opts.duration != 0 and self.opts.duration_mode == 'before-send':
            return self.calculate_delay(self.msg_total_cnt, self.opts.duration)
        return 0

    def set_delay_after(self):
        """
        returns delay duration after sending a message (in seconds)

        :return: time to wait after message is sent/received
        :rtype: float
        """
        if self.opts.duration != 0 and self.opts.duration_mode == 'after-send':
            return self.calculate_delay(self.msg_total_cnt, self.opts.duration)
        return 0

    @staticmethod
    def calculate_delay(in_count, in_duration):
        """
        calculates delay between sending/receiving messages used by shecduler when requested
        by duration option (uses logic from deprecated utils.sleep4next)

        :param in_count: number of messages to be sent/received
        :type in_count: int
        :param in_duration: send/receive process total execution time
        :type in_duration: int
        :return: computed time to wait before or after message is sent/received
        :rtype: float
        """
        delay = 0.0
        if (in_duration > 0) and (in_count > 0):
            delay = 1.0 * in_duration / in_count
        return delay

    def on_transport_error(self, event):
        """
        handler called when any error related to transport occurs

        .. seealso:: MessagingHandler::on_transport_error

        :param event: reactor event
        :type event: proton.Event
        """
        if self.timeout is not None:
            self.timeout.cancel()


class CustomBackoff(proton.reactor.Backoff):
    """
    a reconnect strategy involving an increasing delay between
    retries, up to a maximum or 60 seconds. This is a modified version
    supporting limit to the number of reconnect attempts before giving up.
    """

    def __init__(self, interval=None, limit=None, timeout=None):
        """ CustomBackoff constructor

        :param interval: time interval between re-connect attempts
        :type interval: float
        :param limit:
        :type limit: int
        :param timeout:
        :type timeout: int
        """
        super(CustomBackoff, self).__init__()
        self.failed = 0
        self.cumulative = 0
        self.interval = interval
        self.limit = limit or 0
        self.timeout = timeout or 0

    def reset(self):
        ''' resets the reconnect attempts counters '''
        self.failed = 0
        self.cumulative = 0
        super(CustomBackoff, self).reset()

    def next(self):
        '''
        implements the reconnect attempt action

        :return: next reconnect attempt delay time
        :rtype: float
        '''
        current = self.delay
        if current == 0:
            self.delay = self.interval or 0.1
        else:
            if self.interval is None:
                self.delay = min(60, 2 * current)
        self.failed += 1
        if self.limit and self.failed > self.limit:
            raise Exception('Failed to reconnect within defined %i attempts limit' % self.limit)
        if self.timeout and self.cumulative >= self.timeout:
            raise Exception('Failed to reconnect within defined %.f seconds timeout' % self.timeout)
        print('Connection error: reconnect attempt %i of %i, next in attempt: %.1f seconds'
              % (self.failed, self.limit, current))
        self.cumulative += current
        return current


class DelayedNoop(object):  # pylint: disable=too-few-public-methods
    """ callback object that does nothing. """

    def on_timer_task(self, _):
        """ empty event handler method"""
        pass


class ClientException(Exception):
    """ custom client exception class (just to know source of exception) """

    def __init__(self, message):
        """
        ClientException constructor

        :param message: exception text to be printed out
        :type message: str
        """
        super(ClientException, self).__init__(message)


class ErrorsHandler(object):  # pylint: disable=too-few-public-methods
    """ class to be used as universal errors handler for clients """

    def __init__(self, conn_reconnect):
        """
        ErrorsHandler Constructor

        :param conn_reconnect: indicates whether re-connect is enabled ('true' or 'false')
        :type conn_reconnect: str
        """
        self.conn_reconnect = conn_reconnect

    def on_unhandled(self, name, event):
        """
        Universal handler which sees all events when added as global handler to reactor.

        .. seealso::
           node_data/clients/python/receiver.py
           and node_data/clients/python/sender.py

        :param name: event name
        :type name: str
        :param event: event object
        :type event: proton.Event
        """

        dlv = event.delivery
        # trx_declare_failed
        if ((dlv is not None)
                and (hasattr(event, "transaction"))
                and (event.transaction is not None)
                and (dlv == event.transaction._declare)  # pylint: disable=protected-access
                and (dlv.remote_state == proton.Delivery.REJECTED)):
            raise ClientException("Transaction error: %s ..." % "Transaction declare failed")

        # trx_failed
        if ((dlv is not None)
                and (hasattr(event, "transaction"))
                and (event.transaction is not None)
                and (dlv == event.transaction._discharge)  # pylint: disable=protected-access
                and (dlv.remote_state == proton.Delivery.REJECTED)
                and (not event.transaction.failed)):
            raise ClientException("Transaction error: %s ..." % "Transaction failed")

        # connection_err
        if ((name == "on_connection_remote_close")
                and event.connection.remote_condition):
            err_message = "Connection error: %s ..." \
                          % self._evaluate_endpoint_error(event.connection, "connection")
            if ("AMQ119031" in err_message
                    or self.conn_reconnect == "false"):
                raise ClientException(err_message)
            else:
                utils.dump_error(err_message)

        # link_err
        if ((name == "on_link_remote_close")
                and event.link.remote_condition):
            raise ClientException("Link error: %s ..."
                                  % self._evaluate_endpoint_error(event.link, "link"))

        # session_err
        if ((name == "on_session_remote_close")
                and event.session.remote_condition):
            raise ClientException("Session error: %s ..."
                                  % self._evaluate_endpoint_error(event.session, "session"))

        # transport_err
        if name == "on_transport_error":
            err_message = "Transport error: %s - %s" % (
                event.transport.condition.name, event.transport.condition.description)
            if ("authentication" in event.transport.condition.description.lower()
                    or self.conn_reconnect == "false"):
                raise ClientException(err_message)
            else:
                utils.dump_error(err_message)

        # select_err
        if name == "on_selectable_error":
            raise ClientException("Selector error: %s ..." % "Selectable error ...")

        # delivery rejected err
        if (name == "on_delivery"
                and ((dlv is not None)
                     and dlv.remote_state == proton.Delivery.REJECTED)):
            raise ClientException("Message delivery error: %s ..." % "REJECTED")

    @staticmethod
    def _evaluate_endpoint_error(endpoint, endpoint_type):
        """
        Evaluate endpoint errors and returns detailed description.

        :param endpoint: endpoint
        :type endpoint: proton.Endpoint
        :param endpoint_type: type of the endpoint
        :type endpoint_type: str
        :return: error message
        :rtype: str
        """
        message = "Unknown endpoint error ..."
        if endpoint.remote_condition:
            message = endpoint.remote_condition.description or endpoint.remote_condition.name
        elif (endpoint.state & proton.Endpoint.LOCAL_ACTIVE
              and endpoint.state & proton.Endpoint.REMOTE_CLOSED):
            message = "%s closed by peer" % endpoint_type
        return message
