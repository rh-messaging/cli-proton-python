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

""" Proton reactive API python sender client """

from __future__ import absolute_import, print_function, division

import re
import sys

import proton
import proton.reactor
import proton.handlers

from cli_proton_python import coreclient, options, utils


class Timeout(object):  # pylint: disable=too-few-public-methods
    """ Scheduler object for timeout control """

    def __init__(self, parent, event):
        """
        Timeout constructor
        saving parent_ptr to get current data
        :param parent: related sender object
        :type parent: cli_proton_python.Send
        :param event: reactor event
        :type event: proton.Event
        """
        self.parent_ptr = parent
        self.parent_event = event

    def on_timer_task(self, _):
        """ on_timer_task action handler
        """
        # Stops container
        if self.parent_event.container:
            self.parent_event.container.stop()

        self.parent_ptr.tear_down(self.parent_event)


class Send(coreclient.CoreClient):
    """ Proton reactive API python sender client

    implements various handler methods for reactor events triggered by proton.reactor
    """

    def __init__(self, opts):
        """
        Send constructor

        :param opts: sender client options
        :type opts: optparse.Values instance
        """
        reactor_opts = {}
        if opts.reactor_auto_settle_off:
            reactor_opts['auto_settle'] = False
        if opts.reactor_peer_close_is_error:
            reactor_opts['peer_close_is_error'] = True
        super(Send, self).__init__(opts, reactor_opts)
        self.msg_sent_cnt = 0
        self.msg_released_cnt = 0
        self.msg_confirmed_cnt = 0
        self.msg_total_cnt = opts.count
        self.msg_content_fmt = False
        self.msg = None
        self.msg_content = None
        self.event = None  # sendable event
        self.link_opts = self.parse_link_options()
        self.delay_before = self.set_delay_before()
        self.delay_after = self.set_delay_after()

    @staticmethod
    def prepare_content_from_file(filename):
        """
        reads and returns file contents

        :param filename: path to file to be opened and read
        :type filename: str
        :return: contents of filename as unicode string
        :rtype: str (unicode) or None
        """
        try:
            content_file = open(filename, "r")
            content = content_file.read()
            content_file.close()
        except IOError as exc:
            utils.dump_error(exc)
            return None

        content = content.rstrip()
        try:
            # python2.x convert to unicode
            return content.decode(sys.getfilesystemencoding())
        except AttributeError:
            return content

    def prepare_list_content(self):
        """
        prepares list content

        :return: list constructed from options list items
        :rtype: list
        """
        content = []
        if self.opts.msg_list_items != ['']:
            for item in self.opts.msg_list_items:
                if self.opts.content_type:
                    if item.startswith('~'):
                        item = item[1:]
                    content.append(utils.retype_content(item, self.opts.content_type))
                elif item.startswith('~'):
                    content.append(utils.hard_retype(item[1:]))
                else:
                    content.append(item)
        return content

    def prepare_map_content(self):
        """
        prepares map content

        :return: flat map constructed from options map items
        :rtype: dict
        """
        content = {}
        if self.opts.msg_map_items != ['']:
            content = utils.prepare_flat_map(self.opts.msg_map_items, self.opts.content_type)
        return content

    def prepare_string_content(self, content):
        """
        prepares string content

        re-types content accoding content-type given,
        enables message sequence numbering if formatting string (%[ 0-9]*d) is found

        :param content: message content string
        :type content: str (unicode)
        :return: string message content
        :rtype: str (unicode)
        """
        if content is not None and re.search('%[ 0-9]*d', content) is not None:
            self.msg_content_fmt = True
        if self.opts.content_type:
            return utils.retype_content(content, self.opts.content_type)
        return content

    def prepare_content(self):
        """
        prepares the content depending on type

        .. note::
           * if self.opts.msg_list_items are set amqp/map content is constructed,
           * elif self.opts.msg_map_items are set amqp/list content is constructed,
           * else the content is considered as text/plain

        :return: string message content
        :rtype: str (unicode) or list or dict
        """

        if self.opts.msg_content:
            content = self.opts.msg_content
        elif self.opts.msg_content_from_file:
            content = self.prepare_content_from_file(self.opts.msg_content_from_file)
        else:
            content = None

        if self.opts.msg_list_items:
            return self.prepare_list_content(), "amqp/list"
        elif self.opts.msg_map_items:
            return self.prepare_map_content(), "amqp/map"
        return self.prepare_string_content(content), "text/plain"

    def prepare_message(self):
        """
        compose and return the message

        :return: message to be sent
        :rtype: proton.Message
        """

        msg = proton.Message()
        msg_content, msg_content_type = self.prepare_content()

        if self.opts.msg_durable.lower() == "yes" or self.opts.msg_durable.lower() == "true":
            msg.durable = True
        if self.opts.msg_priority is not None:
            msg.priority = self.opts.msg_priority
        if self.opts.msg_id is not None:
            msg.id = self.opts.msg_id
        if self.opts.msg_correlation_id is not None:
            msg.correlation_id = self.opts.msg_correlation_id
        if self.opts.msg_user_id is not None:
            msg.user_id = self.opts.msg_user_id.encode('utf-8')
        if self.opts.msg_group_id is not None:
            msg.group_id = self.opts.msg_group_id
        if self.opts.msg_group_seq:
            msg.group_sequence = self.opts.msg_group_seq
        if self.opts.msg_reply_to is not None:
            msg.reply_to = self.opts.msg_reply_to
        if self.opts.msg_subject is not None:
            msg.subject = self.opts.msg_subject
        if self.opts.msg_ttl is not None:
            msg.ttl = self.opts.msg_ttl / 1000
        if self.opts.msg_content_type is not None:
            msg.content_type = self.opts.msg_content_type
        if self.opts.msg_address is not None:
            msg.address = self.opts.msg_address
        else:
            msg.content_type = msg_content_type
        msg.properties = utils.prepare_flat_map(self.opts.msg_properties)
        msg.body = msg_content

        return msg

    def schedule_timeout(self, event):
        """
        Cancels an existing timeout (if one exists).
        Next it schedules a new timeout if "--timeout" provided.
        :param event: Reactor event
        :type event: proton.Event
        """
        # Cancels an existing timeout
        if self.timeout:
            self.timeout.cancel()

        # Schedules a new timeout if --timeout provided.
        if self.opts.timeout:
            self.timeout = event.reactor.schedule(self.opts.timeout, Timeout(self, event))

    def send_message(self):
        """ sends a message """
        # close the connection if nothing to send
        if self.msg_total_cnt == 0:
            self.event.connection.close()

        if self.msg_content_fmt:
            self.msg.body = self.msg_content % self.msg_sent_cnt

        self.event.sender.send(self.msg)
        self.msg_sent_cnt += 1

        self.print_message(self.msg)

        # set up a new timeout (if one provided)
        self.schedule_timeout(self.event)

        if self.opts.log_stats == 'endpoints':
            utils.dump_event(self.event)

        if self.opts.link_at_most_once and self.msg_sent_cnt - self.msg_released_cnt == self.msg_total_cnt:
            self.tear_down(self.event)

    def on_start(self, event):
        """
        called when the event loop starts, creates a sender for given url

        :param event: reactor event
        :type event: proton.Event
        """

        if len([(opt, val) for opt, val in self.opts.__dict__.items()
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
            event.container.create_sender(conn, self.url.path, options=self.link_opts)
        else:
            event.container.create_sender(self.url, options=self.link_opts)

        # Schedule a timeout (if needed)
        self.schedule_timeout(event)

    def on_sendable(self, event):
        """
        called when sending can proceed

        :param event: reactor event
        :type event: proton.Event
        """
        if self.event is None:
            self.msg = self.prepare_message()
            self.msg_content = self.msg.body
            self.event = event
            # we are sending first message or first after we previously ran out of credit
            if not self.tearing_down and self.msg_sent_cnt - self.msg_released_cnt < self.msg_total_cnt:
                if self.msg_sent_cnt != 0:
                    # we previously ran out of credit
                    self.next_task = event.reactor.schedule(0, self)
                else:
                    self.next_task = event.reactor.schedule(self.delay_before, self)

    def on_accepted(self, event):
        """
        called when the remote peer accepts an outgoing message

        :param event: reactor event
        :type event: proton.Event
        """
        self.msg_confirmed_cnt += 1
        if self.msg_confirmed_cnt == self.msg_total_cnt:
            self.tear_down(event)

    def on_rejected(self, event):
        """
        called when the remote peer rejects an outgoing message

        :param event: reactor event
        :type event: proton.Event
        """
        self.tear_down(event)

    def on_settled(self, event):
        """
        called when the remote peer has settled the outgoing message,
        this is the point at which it should never be retransmitted

        :param event: reactor event
        :type event: proton.Event
        """
        if not self.auto_settle and self.msg_confirmed_cnt == self.msg_total_cnt:
            self.tear_down(event, settled=True)

    def on_released(self, event):
        """
        called when message is released by the remote container disconnects before
        message has been confirmed
        :param event:
        :return:
        """
        if self.opts.on_release == 'fail':
            raise coreclient.ClientException('Message released: %s' % event.delivery.tag)
        elif self.opts.on_release == 'retry':
            # incrementing count of messages released only when action is retry
            # if action is ignore, the released count will remain untouched to preserve
            # original behavior of the sender client
            self.msg_released_cnt += 1

            # at this point message was released after DelayedNoop timer task was scheduled
            # and so we should reschedule a resend or sender will get stuck, since self.event is assigned
            if not self.tearing_down and self.event and self.msg_sent_cnt >= self.msg_total_cnt:
                self.next_task = self.event.reactor.schedule(self.delay_before + self.delay_after, self)

    def on_disconnected(self, event):
        """
        called when the socket is disconnected

        :param event: reactor event
        :type event: proton.Event
        """
        if not self.opts.link_at_most_once:
            self.msg_sent_cnt = self.msg_confirmed_cnt

    def on_timer_task(self, _):
        """
        next send action scheduler

        the Send object itself is scheduled to perform next send action, that is why it contains
        timer_task method

        incoming event object does not contain many fields, that is why self.event is used
        """
        # can send message
        if self.event and self.event.sender.credit and self.msg_sent_cnt - self.msg_released_cnt < self.msg_total_cnt:
            self.send_message()
        else:
            self.event = None
            return
        # should send more messages
        if not self.tearing_down and self.msg_sent_cnt - self.msg_released_cnt < self.msg_total_cnt:
            self.next_task = self.event.reactor.schedule(self.delay_before + self.delay_after, self)
        else:
            self.event.reactor.schedule(self.delay_after, coreclient.DelayedNoop())


class TxSend(Send, proton.handlers.TransactionHandler):
    """
    Proton reactive API python transactional sender client

    implements various handler methods for reactor events triggered by proton.reactor
    """

    def __init__(self, opts):
        """
        TxSend constructor

        :param opts: sender client options
        :type opts: optparse.Values instance
        """
        super(TxSend, self).__init__(opts)
        if not self.auto_settle:
            raise NotImplementedError("Manual settling not supported in TX mode")
        self.current_batch = 0
        self.msg_processed_cnt = 0
        self.sender = None

    def transaction_process(self, event):
        """
        transactionally send a message, process reporting and control options

        :param event: reactor event
        :type event: proton.Event
        """
        msg = self.prepare_message()
        if self.msg_content_fmt:
            msg_content = msg.body
        while event.transaction and self.sender.credit and (
                self.msg_processed_cnt + self.current_batch) < self.msg_total_cnt:
            if self.msg_content_fmt:
                msg.body = msg_content % (self.msg_processed_cnt + self.current_batch)
            if self.opts.duration != 0 and self.opts.duration_mode == 'before-send':
                utils.sleep4next(self.start_tm, self.msg_total_cnt, self.opts.duration,
                                 self.msg_processed_cnt + self.current_batch + 1)

            event.transaction.send(self.sender, msg)
            self.current_batch += 1

            if self.opts.log_stats == 'endpoints':
                utils.dump_event(event)

            self.print_message(msg)

            # Schedule a timeout (if needed)
            self.schedule_timeout(event)

            if self.opts.duration != 0 and self.opts.duration_mode == 'after-send':
                utils.sleep4next(self.start_tm, self.msg_total_cnt, self.opts.duration,
                                 self.msg_processed_cnt + self.current_batch)

            self.transaction_finish(event)

    def transaction_finish(self, event):
        """
        finish transaction, do tranaction action, process reporting and control options

        :param event: reactor event
        :type event: proton.Event
        """
        if self.current_batch == self.opts.tx_size:
            if self.opts.tx_action == 'commit':
                event.transaction.commit()
            elif self.opts.tx_action == 'rollback':
                event.transaction.abort()
            event.transaction = None

            if self.opts.tx_action == 'none':
                if self.msg_processed_cnt + self.current_batch == self.msg_total_cnt:
                    self.tear_down(event)
                else:
                    self.msg_processed_cnt += self.current_batch
                    self.current_batch = 0
                    event.reactor.declare_transaction(event.connection, handler=self)

            if self.opts.duration != 0 and self.opts.duration_mode == 'after-send-tx-action':
                utils.sleep4next(self.start_tm, self.msg_total_cnt, self.opts.duration,
                                 self.msg_processed_cnt + self.current_batch)
        elif self.msg_processed_cnt + self.current_batch == self.msg_total_cnt:
            if self.opts.tx_endloop_action == 'commit':
                event.transaction.commit()
            elif self.opts.tx_endloop_action == 'rollback':
                event.transaction.abort()
            else:
                self.tear_down(event)

    def on_start(self, event):
        """
        called when the event loop starts, creates a transactional sender for given url

        :param event: reactor event
        :type event: proton.Event
        """
        conn = event.container.connect(self.url)
        self.sender = event.container.create_sender(conn, self.url.path, options=self.link_opts)
        event.container.declare_transaction(conn, handler=self)

        # Schedule a timeout (if needed)
        self.schedule_timeout(event)

    def on_transaction_declared(self, event):
        """
        called when the transaction is declared

        :param event: reactor event
        :type event: proton.Event
        """
        self.transaction_process(event)

    def on_transaction_committed(self, event):
        """
        called when the transaction is committed

        :param event: reactor event
        :type event: proton.Event
        """
        self.msg_processed_cnt += self.current_batch
        if self.msg_processed_cnt == self.msg_total_cnt:
            self.tear_down(event)
        else:
            self.current_batch = 0
            event.reactor.declare_transaction(event.connection, handler=self)

    def on_transaction_aborted(self, event):
        """
        called when the transaction is aborted

        :param event: reactor event
        :type event: proton.Event
        """
        self.msg_processed_cnt += self.current_batch
        if self.msg_processed_cnt == self.msg_total_cnt:
            self.tear_down(event)
        else:
            self.current_batch = 0
            event.reactor.declare_transaction(event.connection, handler=self)

    def on_sendable(self, event):
        """
        suppressed in transactional, no actions are performed

        :param event: reactor event
        :type event: proton.Event
        """
        pass

    def on_accepted(self, event):
        """
        suppressed in transactional, no actions are performed

        :param event: reactor event
        :type event: proton.Event
        """
        pass

    def on_settled(self, event):
        """
        suppressed in transactional, no actions are performed

        :param event: reactor event
        :type event: proton.Event
        """
        pass

    def on_disconnected(self, event):
        """
        suppressed in transactional, no actions are performed

        :param event: reactor event
        :type event: proton.Event
        """
        self.current_batch = 0

    def on_timer_task(self, _):
        """
        suppressed in transactional, no actions are performed

        :param event: reactor event
        :type event: proton.Event
        """
        pass


def main():
    """ main loop """
    ecode = 0
    parser = options.SenderOptions()
    opts, _ = parser.parse_args()

    if opts.log_lib is not None:
        utils.set_up_client_logging(opts.log_lib)

    try:
        # main loop
        if opts.tx_size or opts.tx_endloop_action is not None:
            container = proton.reactor.Container(TxSend(opts))
        else:
            container = proton.reactor.Container(Send(opts))
        super(proton.reactor.Container, container).global_handler.add(
            coreclient.ErrorsHandler(opts.conn_reconnect))
        container.run()
    except Exception:  # pylint: disable=broad-except
        raise
    sys.exit(ecode)


if __name__ == '__main__':
    main()
