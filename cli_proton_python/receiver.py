#!/usr/bin/env python
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

""" Proton reactive API python receiver client """

from __future__ import absolute_import

import sys

import proton.handlers
import proton.reactor

from cli_proton_python import coreclient, options, utils


class Timeout(object):  # pylint: disable=too-few-public-methods
    """ Scheduler object for timeout control """

    def __init__(self, parent, event):
        """
        Timeout constructor

        saving parent_ptr to get current data,
        as well as initial last_known value of received messages count

        :param parent: related receiver object
        :param parent: cli_proton_python.Recv
        :param event: reactor event
        :type event: proton.Event
        """
        self.parent_ptr = parent
        self.parent_event = event
        self.last_known_msg_received_cnt = parent.msg_received_cnt

    def on_timer_task(self, _):
        """ on_timer_task action handler

        if it's time to close, so no new messages arrived in time, close connection
        """
        if self.parent_ptr.msg_received_cnt == self.last_known_msg_received_cnt:
            self.parent_ptr.tear_down(self.parent_event)
        if self.parent_ptr.msg_expected_cnt == 0:
            self.parent_ptr.tear_down(self.parent_event)


class Recv(coreclient.CoreClient):
    """
    Proton reactive API python receiver client

    implements various handler methods for reactor events triggered by proton.reactor
    """

    def __init__(self, opts, prefetch=None):
        """
        Recv constructor

        :param opts: receiver client options
        :type opts: optparse.Values instance
        :param prefetch: prefetch buffer size
        :type prefetch: int
        """
        reactor_opts = {'auto_accept': opts.reactor_auto_accept, 'prefetch': opts.reactor_prefetch}
        if prefetch is not None:
            reactor_opts['prefetch'] = prefetch
        if opts.reactor_peer_close_is_error:
            reactor_opts['peer_close_is_error'] = opts.reactor_peer_close_is_error
        if opts.reactor_auto_settle_off:
            if opts.process_reply_to:
                reactor_opts['auto_settle'] = False
            else:
                raise ValueError("manual settling is only available"
                                 " with --process-reply-to enabled")
        super(Recv, self).__init__(opts, reactor_opts)
        self.msg_expected_cnt = opts.count
        self.msg_received_cnt = 0
        self.senders = {}
        self.relay = None
        self.link_opts = self.parse_link_options()
        # to hold pointer current receiver object
        self.receiver = None
        self.acceptor = None

    def parse_link_options(self):
        """
        Prepare link options

        :return: list of link options
        :rtype: list
        """
        link_opts = super(Recv, self).parse_link_options()
        if self.opts.link_durable:
            link_opts.append(proton.reactor.DurableSubscription())
        if self.opts.recv_browse:
            link_opts.append(proton.reactor.Copy())
        if self.opts.recv_consume:
            link_opts.append(proton.reactor.Move())
        if self.opts.recv_filter:
            link_opts.append(proton.reactor.Filter(utils.prepare_flat_map(self.opts.recv_filter)))
        if self.opts.recv_selector is not None:
            link_opts.append(proton.reactor.Selector(self.opts.recv_selector))
        if self.opts.link_dynamic_node_properties:
            link_opts.append(proton.reactor.DynamicNodeProperties(
                utils.prepare_flat_map(self.opts.link_dynamic_node_properties)
            ))
        return link_opts

    def do_message_action(self, delivery):
        """
        performs requested action on received message

        :param delivery: delivery disposition
        :type delivery: proton.Delivery
        """

        if self.opts.action == "reject":
            self.reject(delivery)
        elif self.opts.action == "release":
            self.release(delivery)
        elif self.opts.action != "noack":
            self.accept(delivery)

    def process_reply_to(self, event):
        """
        sends received message to reply to address

        :param event: reactor event
        :type event: proton.Event
        """
        if event.message.reply_to:
            sender = self.relay or self.senders.get(event.message.reply_to)
            if sender is None:
                sender = event.reactor.create_sender(event.connection, event.message.reply_to)
                self.senders[event.message.reply_to] = sender
            message = event.message
            message.address = event.message.reply_to
            sender.send(message)

    def check_empty(self, event):
        """
        checks whether there are messages to dispatch

        :param event: reactor event
        :type event: proton.Event
        """
        if event.receiver.drain_mode and not event.receiver.credit and not event.receiver.queued:
            self.tear_down(event)
        elif self.opts.timeout and self.msg_received_cnt == self.msg_expected_cnt:
            self.tear_down(event)

    def tear_down(self, event, settled=False):
        """
        tears down and closes the connection

        :param event: reactor event
        :type event: proton.Event
        :param settled: indicates whether all messages has been explicitly settled
        :type settled: bool
        """
        super(Recv, self).tear_down(event, settled)
        if self.acceptor and self.opts.recv_listen:
            self.acceptor.close()

    def on_start(self, event):
        """
        called when the event loop starts, creates a receiver for given url

        :param event: reactor event
        :type event: proton.Event
        """
        if self.opts.recv_listen:
            self.acceptor = event.container.listen(self.url)
        elif len([(opt, val) for opt, val in self.opts.__dict__.items()
                  if opt.startswith('conn') and val is not None]) != 0:
            # some connection options were given
            self.set_up_ssl(event)
            if self.opts.conn_use_config_file:
                conn = event.container.connect()
            else:
                conn_opts = self.parse_connection_options()
                if 'urls' in conn_opts:
                    conn_opts['urls'].insert(0, self.url)
                    conn = event.container.connect(**conn_opts)
                else:
                    conn = event.container.connect(self.url, **conn_opts)
            self.receiver = event.container.create_receiver(conn, self.url.path,
                                                            options=self.link_opts,
                                                            dynamic=self.opts.dynamic)
        else:
            self.receiver = event.container.create_receiver(self.url,
                                                            options=self.link_opts,
                                                            dynamic=self.opts.dynamic)

    def on_connection_opened(self, event):
        """
        called when connection is opened

        :param event: reactor event
        :type event: proton.Event
        """
        if self.opts.process_reply_to and event.connection.remote_offered_capabilities and \
                'ANONYMOUS-RELAY' in event.connection.remote_offered_capabilities.elements:
            self.relay = event.reactor.create_sender(event.connection, None)

    def on_link_opened(self, event):
        """
        called when link is opened

        :param event: reactor event
        :type event: proton.Event
        """
        if event.receiver is None:
            return  # Bail out if this is event for the internal transaction link

        # Timeout on start
        if self.opts.timeout:
            self.timeout = event.reactor.schedule(self.opts.timeout, Timeout(self, event))
            if self.msg_expected_cnt == 0 and (self.opts.tx_size or self.opts.tx_endloop_action):
                event.receiver.drain_mode = True
        elif self.msg_expected_cnt == 0:
            event.receiver.drain_mode = True

    def on_link_flow(self, event):
        """
        called on link flow

        :param event: reactor event
        :type event: proton.Event
        """
        if event.receiver is not None:
            self.check_empty(event)

    def on_delivery(self, event):
        """
        called when a message is delivered

        :param event: reactor event
        :type event: proton.Event
        """
        _ = event
        if self.opts.duration != 0 and self.opts.duration_mode == 'before-receive' \
                and self.msg_received_cnt < self.msg_expected_cnt:
            utils.sleep4next(self.start_tm, self.msg_expected_cnt,
                             self.opts.duration, self.msg_received_cnt + 1)

    def on_settled(self, event):
        """
        called when the remote peer has settled the outgoing message
        this is the point at which it should never be retransmitted

        :param event: reactor event
        :type event: proton.Event
        """
        if not self.auto_settle and self.msg_received_cnt == self.msg_expected_cnt:
            self.tear_down(event, settled=True)

    def on_message(self, event):
        """
        called when a message is received

        :param event: reactor event
        :type event: proton.Event
        """

        if self.msg_expected_cnt == 0 or self.msg_received_cnt < self.msg_expected_cnt:
            self.msg_received_cnt += 1
            if self.opts.log_stats == 'endpoints':
                utils.dump_event(event)

            self.print_message(event.message)

            if self.opts.duration != 0 and self.opts.duration_mode == 'after-receive' \
                    or self.opts.duration_mode == 'after-receive-tx-action':
                utils.sleep4next(self.start_tm, self.msg_expected_cnt,
                                 self.opts.duration, self.msg_received_cnt)

            if not self.opts.link_at_most_once and not self.opts.reactor_auto_accept:
                if self.msg_received_cnt % self.opts.action_size == 0:
                    self.do_message_action(event.delivery)
                if self.opts.duration != 0 and self.opts.duration_mode == 'after-receive-action':
                    utils.sleep4next(self.start_tm, self.msg_expected_cnt,
                                     self.opts.duration, self.msg_received_cnt)

            if self.opts.process_reply_to:
                self.process_reply_to(event)

            if self.msg_received_cnt == self.msg_expected_cnt:
                self.tear_down(event)

            self.check_empty(event)

        # timeout if there is nothing to read
        if self.receiver and self.receiver.queued == 0 and self.opts.timeout:
            self.timeout.cancel()
            if self.msg_received_cnt != self.msg_expected_cnt:
                self.timeout = event.reactor.schedule(self.opts.timeout, Timeout(self, event))


class TxRecv(Recv, proton.handlers.TransactionHandler):
    """
    Proton reactive API python transactional receiver client

    implements various handler methods for reactor events triggered by proton.reactor
    """

    def __init__(self, opts):
        """
        TxRecv constructor

        :param opts: receiver client options
        :type opts: optparse.Values instance
        """
        super(TxRecv, self).__init__(opts, prefetch=0)
        if not self.auto_settle:
            raise NotImplementedError("Manual settling not supported in TX mode")
        self.current_batch = 0
        self.msg_processed_cnt = 0
        self.transaction = None
        self.receiver = None
        self.received = 0
        self.default_batch = 10

    def is_empty(self, event):
        """
        check if queue is empty

        :param event: reactor event
        :type event: proton.Event
        :return: True if the source is empty, False otherwise
        :rtype: bool
        """
        if event.receiver.drain_mode and not event.receiver.queued and not event.receiver.credit:
            if self.received <= self.msg_processed_cnt + self.current_batch:
                return True
        return False

    def transaction_process(self, event):
        """
        transactionally receive a message, process reporting and control options

        :param event: reactor event
        :type event: proton.Event
        """
        self.transaction.accept(event.delivery)
        self.current_batch += 1
        self.received += 1
        if self.opts.log_stats == 'endpoints':
            utils.dump_event(event)

        self.print_message(event.message)

        if self.opts.duration != 0 and self.opts.duration_mode == 'after-receive':
            utils.sleep4next(self.start_tm, self.msg_expected_cnt, self.opts.duration,
                             self.msg_processed_cnt + self.current_batch)

        if not self.opts.link_at_most_once and not self.opts.reactor_auto_accept:
            if (self.msg_processed_cnt + self.current_batch) % self.opts.action_size == 0:
                self.do_message_action(event.delivery)
            if self.opts.duration != 0 and self.opts.duration_mode == 'after-receive-action':
                utils.sleep4next(self.start_tm, self.msg_expected_cnt, self.opts.duration,
                                 self.msg_processed_cnt + self.current_batch)

        if self.opts.process_reply_to:
            self.process_reply_to(event)

        self.transaction_finish(event)

    def transaction_finish(self, event):
        """
        finish transaction, do tranaction action, process reporting and control options

        :param event: reactor event
        :type event: proton.Event
        """
        if self.current_batch == self.opts.tx_size:
            if self.opts.tx_action == 'commit':
                self.transaction.commit()
            elif self.opts.tx_action == 'rollback':
                self.transaction.abort()

            if self.opts.tx_action == 'none':
                if self.msg_processed_cnt + self.current_batch == self.msg_expected_cnt:
                    self.tear_down(event)
                else:
                    self.msg_processed_cnt += self.current_batch
                    self.current_batch = 0
                    event.reactor.declare_transaction(event.connection, handler=self)

            if self.opts.duration != 0 and self.opts.duration_mode == 'after-receive-tx-action':
                utils.sleep4next(self.start_tm, self.msg_expected_cnt, self.opts.duration,
                                 self.msg_processed_cnt + self.current_batch)
        elif ((self.msg_expected_cnt and
               self.msg_processed_cnt + self.current_batch == self.msg_expected_cnt) or
              (not self.msg_expected_cnt and self.is_empty(event))):
            if self.opts.tx_endloop_action == 'commit':
                self.transaction.commit()
            elif self.opts.tx_endloop_action == 'rollback':
                self.transaction.abort()
            else:
                self.tear_down(event)

    def on_start(self, event):
        """
        called when the event loop starts, creates a transactional receiver for given url

        :param event: reactor event
        :type event: proton.Event
        """
        conn = event.container.connect(self.url)
        self.receiver = event.container.create_receiver(conn, self.url.path,
                                                        options=self.link_opts,
                                                        dynamic=self.opts.dynamic)
        event.container.declare_transaction(conn, handler=self)
        self.transaction = None

    def on_delivery(self, event):
        """
        called when a message is delivered

        :param event: reactor event
        :type event: proton.Event
        """
        if self.opts.duration != 0 and self.opts.duration_mode == 'before-receive':
            utils.sleep4next(self.start_tm, self.msg_expected_cnt,
                             self.opts.duration, self.msg_processed_cnt + self.current_batch + 1)

    def on_message(self, event):
        """
        called when a message is received

        :param event: reactor event
        :type event: proton.Event
        """
        self.transaction_process(event)

    def on_transaction_declared(self, event):
        """
        called when the transaction is declared

        :param event: reactor event
        :type event: proton.Event
        """
        if (self.msg_expected_cnt and
                (self.msg_processed_cnt + self.opts.tx_size) > self.msg_expected_cnt):
            batch_size = self.msg_expected_cnt % self.opts.tx_size
        elif self.msg_expected_cnt:
            batch_size = self.msg_expected_cnt
        else:
            if self.opts.tx_size:
                batch_size = self.opts.tx_size
            else:
                batch_size = self.default_batch
        self.receiver.flow(batch_size)
        self.transaction = event.transaction

    def on_transaction_committed(self, event):
        """
        called when the transaction is committed

        :param event: reactor event
        :type event: proton.Event
        """
        self.msg_processed_cnt += self.current_batch
        self.current_batch = 0
        if self.msg_expected_cnt == 0 or self.msg_processed_cnt < self.msg_expected_cnt:
            event.reactor.declare_transaction(event.connection, handler=self)
        else:
            self.tear_down(event)

    def on_transaction_aborted(self, event):
        """
        called when the transaction is aborted

        :param event: reactor event
        :type event: proton.Event
        """
        self.msg_processed_cnt += self.current_batch
        self.current_batch = 0
        if self.msg_expected_cnt == 0 or self.msg_processed_cnt < self.msg_expected_cnt:
            event.reactor.declare_transaction(event.connection, handler=self)
        else:
            self.tear_down(event)

    def on_disconnected(self, _):
        """ called when the transaction is disconnected """
        self.current_batch = 0


def main():
    """ main loop """
    ecode = 0
    parser = options.ReceiverOptions()
    opts, _ = parser.parse_args()

    if opts.log_lib is not None:
        utils.set_up_client_logging(opts.log_lib)

    try:
        # main loop
        if opts.tx_size or opts.tx_endloop_action is not None:
            container = proton.reactor.Container(TxRecv(opts))
        else:
            container = proton.reactor.Container(Recv(opts))
        super(proton.reactor.Container, container).global_handler.add(
            coreclient.ErrorsHandler(opts.conn_reconnect))
        container.run()
    except Exception:  # pylint: disable=broad-except
        raise
    sys.exit(ecode)


if __name__ == '__main__':
    main()
