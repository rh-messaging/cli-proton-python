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

""" Proton reactive API python connector client """

from __future__ import absolute_import, print_function

import sys
import threading
import time

import proton
import proton.reactor

from cli_proton_python import coreclient, options, utils


class Connector(coreclient.CoreClient):
    """ Proton reactive API python connector client """

    def __init__(self, opts):
        """
        Connector constructor

        :param opts: connector client options
        :type opts: optparse.Values instance
        """
        super(Connector, self).__init__(opts)
        if self.url.path is not None and self.opts.obj_ctrl in ['C', 'CE']:
            self.opts.obj_ctrl = "CESR"
        self.connection = None
        self.session = None
        self.sender = None
        self.receiver = None
        self.result = {
            'connection': {'open': 0, 'error': 0},
            'session': {'open': 0},
            'link': {
                'open': 0,
                'sender': {'open': 0},
                'receiver': {'open': 0}
            }
        }

    def close_objects(self):
        """
        closes all the open objects (after given close-sleep time)
        """
        time.sleep(self.opts.close_sleep)
        if "R" in self.opts.obj_ctrl:
            self.receiver.close()
        if "S" in self.opts.obj_ctrl:
            self.sender.close()
        if self.opts.obj_ctrl == "CE":
            self.session.close()
        if "C" in self.opts.obj_ctrl:
            self.connection.close()

    def on_start(self, event):
        """
        called when the event loop starts

        :param event: reactor event
        :type event: proton.Event
        """
        if self.opts.conn_use_config_file:
            self.connection = event.container.connect()
        else:
            conn_opts = self.parse_connection_options()
            conn_opts['reconnect'] = False
            if 'urls' in conn_opts:
                conn_opts['urls'].insert(0, self.url)
                self.connection = event.container.connect(**conn_opts)
            else:
                self.connection = event.container.connect(self.url, **conn_opts)

        if "E" in self.opts.obj_ctrl:
            self.session = self.connection.session()
            if "S" in self.opts.obj_ctrl:
                self.sender = event.container.create_sender(self.connection, self.url.path)
            if "R" in self.opts.obj_ctrl:
                self.receiver = event.container.create_receiver(self.connection, self.url.path)

    def on_transport_error(self, event):
        """
        called when the connection can't be opened due to transport error

        :param event: reactor event
        :type event: proton.Event
        """
        _ = event  # ignore incomming event
        self.close_objects()

    def on_link_opened(self, event):
        """
        called when the link is opened

        :param event: reactor event
        :type event: proton.Event
        """
        self.result['link']['open'] += 1
        if event.link.is_sender:
            self.result['link']['sender']['open'] += 1
        if event.link.is_receiver:
            self.result['link']['receiver']['open'] += 1

        if 'SR' in self.opts.obj_ctrl:
            if self.result['link']['open'] == 2:
                self.close_objects()
        else:
            self.close_objects()

    def on_session_opened(self, event):
        """ called when the session is opened

        :param event: reactor event
        :type event: proton.Event
        """
        _ = event  # ignore incomming event
        self.result['session']['open'] += 1
        if 'S' not in self.opts.obj_ctrl and 'R' not in self.opts.obj_ctrl:
            self.close_objects()

    def on_connection_opened(self, event):
        """
        called when the connection is opened

        :param event: reactor event
        :type event: proton.Event
        """
        _ = event  # ignore incomming event
        if 'E' not in self.opts.obj_ctrl:
            self.close_objects()
        if self.opts.obj_ctrl == 'CE':
            self.session.open()

    def on_connection_remote_open(self, event):
        """
        called when the remote connection is opening

        :param event: reactor event
        :type event: proton.Event
        """
        if event.connection.state == proton.Endpoint.LOCAL_ACTIVE + proton.Endpoint.REMOTE_ACTIVE:
            self.result['connection']['open'] += 1

    def get_result(self):
        """
        called when the reactor's exit

        :return: error message
        :rtype: str
        """
        return self.result

    def get_conn_result(self):
        """
        returns the connection statistics triplet

        | connection statistictriplets are:
        | * connections opened, connections errors, connection requests
        | * connection requests stat is ignored, set to 1 for backwards compatibility

        :return: connector statistics triplet
        :rtype: tuple
        """
        return self.result['connection']['open'], self.result['connection']['error'], 1


def run_connectors(opts, results, errors, stats=None):
    """
    thread worker function

    :param opts: connector client options
    :type opts: optparse.Values instance
    :param results: list of connection results
    :type results: list
    :param errors: number of connection errors
    :type errors: int
    :param stats: list containing statistics dictionary (default: None)
    :type stats: list
    """
    simple_connector = Connector(opts)
    try:  # nested try is Python 2.4 compatibility construct
        try:  # see https://docs.python.org/2/whatsnew/2.5.html#pep-341-unified-try-except-finally
            container = proton.reactor.Container(simple_connector)
            super(proton.reactor.Container, container).global_handler.add(
                coreclient.ErrorsHandler(opts.conn_reconnect))
            container.run()
        except coreclient.ClientException as exc:
            simple_connector.result['connection']['error'] = 1
            errors.append(str(exc))
    finally:
        results.append(simple_connector.get_conn_result())
        if stats is not None:
            stats.append(simple_connector.get_result())


def main():
    """ main loop """
    parser = options.ConnectorOptions()
    opts, _ = parser.parse_args()

    ecode = 0
    threads = []
    results = []
    errors = []
    stats = None
    opts.conn_reconnect = "false"

    if opts.log_stats == 'connector':
        stats = []

    if opts.log_lib is not None:
        utils.set_up_client_logging(opts.log_lib)

    try:
        # main loop
        for i in range(opts.count):
            connector = threading.Thread(target=run_connectors, args=(opts, results, errors, stats))
            threads.append(connector)
            connector.start()

        for i in range(len(threads)):
            threads[i].join()

    except Exception:  # pylint: disable=broad-except
        ecode = 1

    result = [sum(i_res) for i_res in zip(*results)]
    ecode = (len(result) == 3 and result[1] > 0) or ecode
    stdout = ' '.join(str(val) for val in result)

    for err in errors:
        utils.dump_error(err)

    if opts.log_stats == 'connector':
        for stat in stats:
            print("STATS", stat)
    print(stdout)
    sys.exit(ecode)


if __name__ == '__main__':
    main()
