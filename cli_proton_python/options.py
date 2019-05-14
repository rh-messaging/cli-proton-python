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

""" Proton reactive API python client options module"""

from __future__ import absolute_import

import optparse
import sys


### Python 2.x cmdline options to unicode conversion callback functions

def convert_to_unicode(value):
    """
    Python 2.x: converts value to unicode

    :param value: value to be converted to unicode
    :type value: str
    :return: unicode string
    :rtype: str (unicode)
    """
    try:
        return value.decode(sys.getfilesystemencoding())
    except AttributeError:
        return value

def to_unicode(option, _, value, parser):
    """ stores values of multi-value cmdline string, converts to unicode for Python 2.x

    :param option: option object
    :type value: optparse.Option
    :param value: option value
    :type value: str
    :param value: option parser
    :type value: related Option object from cli_proton_python.options
    """
    lst = getattr(parser.values, option.dest) or []
    int_value = convert_to_unicode(value)
    lst.append(int_value)
    setattr(parser.values, option.dest, lst)

def str_to_unicode(option, _, value, parser):
    """
    Python 2.x: stores cmdline string, converts to unicode for Python 2.x

    :param option: option object
    :type value: optparse.Option
    :param value: option value
    :type value: str
    :param value: option parser
    :type value: related Option object from cli_proton_python.options
    """
    setattr(parser.values, option.dest, convert_to_unicode(value))


class CoreOptions(optparse.OptionParser, object):
    """ Proton reactive API python core client options """

    def __init__(self):
        """ CoreOptions constructor """
        super(CoreOptions, self).__init__()

    def add_control_options(self):
        """ add the control options """
        group = optparse.OptionGroup(self, "Control Options")
        group.add_option("-b", "--broker-url", type="string", default="localhost:5672/examples",
                         help="url broker to connect to (default %default)")
        group.add_option("-c", "--count", type="int", default=0,
                         help="number of messages to be sent/received (default %default)")
        group.add_option("-t", "--timeout", type="float",
                         help="timeout in seconds to wait before exiting (default %default)")
        group.add_option("--close-sleep", type="int", default=0,
                         help="sleep before sender/receiver/session/connection.close() "
                              "(default %default)")
        group.add_option("--sync-mode", type="choice",
                         help="synchronization mode", choices=['none', 'session', 'action'])
        self.add_option_group(group)

    def add_connection_options(self):
        """ add the connection options """
        group = optparse.OptionGroup(self, "Connection Options")
        group.add_option("--conn-use-config-file", action="store_true",
                         help='use configuration file for connection')
        group.add_option("--conn-urls", type="string",
                         help='define connection urls')
        group.add_option("--conn-reconnect", type="choice", default='true',
                         help='client reconnect settings (default %default)',
                         choices=['true', 'false', 'True', 'False'], action='callback',
                         callback=lambda option, opt_str, value, parser: setattr(parser.values,
                                                                                 option.dest,
                                                                                 value.lower()))
        group.add_option("--conn-reconnect-interval", type="float",
                         help='client reconnect interval '
                              '(specifying this option implies custom reconnect, default %default)')
        group.add_option("--conn-reconnect-limit", type="int",
                         # default value set later to distinguish default from explicit
                         help='client reconnect limit '
                              '(specifying this option implies custom reconnect, default 99)')
        group.add_option("--conn-reconnect-timeout", type="int",
                         help='client reconnect limit '
                              '(specifying this option implies custom reconnect, default %default)')
        group.add_option("--conn-heartbeat", type="int",
                         help='enable and set connection heartbeat (seconds)')
        group.add_option("--conn-ssl-certificate", type="string",
                         help='path to client certificate '
                              '(PEM format), enables client authentication')
        group.add_option("--conn-ssl-private-key", type="string",
                         help='path to client private key (PEM format), '
                              'conn-ssl-certificate must be given')
        group.add_option("--conn-ssl-password", type="string",
                         help="client's certificate database password")
        group.add_option("--conn-ssl-trust-store", type="string",
                         help='path to client trust store (PEM format), '
                              'conn-ssl-certificate must be given')
        group.add_option("--conn-ssl-verify-peer", action="store_true",
                         help='verifies server certificate, conn-ssl-certificate '
                              'and trusted db path needs to be specified (PEM format)')
        group.add_option("--conn-ssl-verify-peer-name", action="store_true",
                         help='verifies connection url against server hostname')
        group.add_option("--conn-handler", type="string",
                         help='define custom connection handler')
        group.add_option("--conn-max-frame-size", type=int,
                         help='define custom maximum frame size in bytes (range: 512-4294967295)')
        group.add_option("--conn-sasl-enabled", type="choice", default='true',
                         help='enable connection SASL (default %default)',
                         choices=['true', 'false', 'True', 'False'], action='callback',
                         callback=lambda option, opt_str, value, parser: setattr(parser.values,
                                                                                 option.dest,
                                                                                 value.lower()))
        group.add_option("--conn-allowed-mechs", type="string",
                         help='Define custom Allowed SASL mechanism list, '
                              'separated by space e.g. "GSSAPI PLAIN"')
        self.add_option_group(group)

    def add_logging_options(self):
        """ add the logging options """
        group = optparse.OptionGroup(self, "Logging Options")
        group.add_option("--log-lib", type="choice",
                         help="enable client library logging (default %default)",
                         choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG',
                                  'TRANSPORT_RAW', 'TRANSPORT_FRM', 'TRANSPORT_DRV'])
        group.add_option("--log-stats", type="choice", choices=['endpoints'],
                         help="report various statistic/debug information")
        self.add_option_group(group)


class SRCoreOptions(CoreOptions):
    """ Proton reactive API python sender/receiver client options """

    def __init__(self):
        """ SRCoreOptions cconstructor """
        super(SRCoreOptions, self).__init__()

    def add_control_options(self):
        """ add the control options """
        super(SRCoreOptions, self).add_control_options()
        group = [group for group in self.option_groups if group.title == "Control Options"][0]
        group.add_option("--duration", type="int", default=0,
                         help="message actions total duration "
                              "(defines msg-rate together with count, default %default)")
        group.add_option("--duration-mode", type="choice", choices=[],
                         help="in use with --duration defines where to wait (default %default) "
                              "**NOTE: 'after-send/receive-tx-action' "
                              "is equal to 'after-send/receive' if not in transactional mode "
                              "(tx-size or tx-endloop-action given)")
        group.add_option("--capacity", type="int",
                         help="session's capacity (default %default)")

    def add_logging_options(self):
        """ add the logging options """
        super(SRCoreOptions, self).add_logging_options()
        group = [group for group in self.option_groups if group.title == "Logging Options"][0]
        group.add_option("--log-msgs", type="choice", action="store",
                         choices=['dict', 'body', 'upstream', 'none', 'interop', 'json'],
                         help="message[s] reporting style (default %default)")

    def add_transaction_options(self):
        """ add the transaction options """
        group = optparse.OptionGroup(self, "Transaction Options")
        group.add_option("--tx-size", type="int", default=0,
                         help="transactional mode: batch message count size")
        group.add_option("--tx-action", type="choice", default='commit',
                         choices=['commit', 'rollback', 'none'],
                         help="transactional action at the end of tx batch (default %default)")
        group.add_option("--tx-endloop-action", type="choice",
                         choices=['commit', 'rollback', 'none'],
                         help="transactional action after sending all messages in loop "
                              "(default %default)")
        self.add_option_group(group)

    def add_link_options(self):
        """ add the link options """
        group = optparse.OptionGroup(self, "Link Options")
        group.add_option("--link-durable", action="store_true",
                         help='use durable subscription')
        group.add_option("--link-at-least-once", action="store_true",
                         help='reliable delivery')
        group.add_option("--link-at-most-once", action="store_true",
                         help='best-effort delivery')
        self.add_option_group(group)


class ConnectorOptions(CoreOptions):
    """ Proton reactive API python connector specific client options """

    def __init__(self):
        """ ConnectorOptions cconstructor """
        super(ConnectorOptions, self).__init__()
        self.add_control_options()
        self.add_logging_options()
        self.add_connection_options()
        self.add_connector_options()
        self.set_default("broker_url", "localhost:5672")
        self.set_default("count", 1)
        self.get_option('--count').help = "Specify how many connection/sessions/senders/receivers" \
                                          " connector tries to create and open (default %default)"
        self.get_option('--close-sleep').help = "Opened objects will be held" \
                                                " till duration passes by"
        self.get_option("--log-stats").choices = ['connector']

    def add_connector_options(self):
        """ add the connector options """
        group = optparse.OptionGroup(self, "Connector options")
        group.add_option("--obj-ctrl", type="choice",
                         default='C', choices=['C', 'CE', 'CES', 'CER', 'CESR'],
                         help="Optional creation object control based on <object-ids> "
                              "syntax C/E/S/R stands for Connection, sEssion, Sender, Receiver "
                              "e.g. --obj-ctrl \"CES\" for creation of Connection+sEssion+Sender "
                              "(default: %default (address not given), 'CESR' (address specified))")
        self.add_option_group(group)


class SenderOptions(SRCoreOptions):
    """ Proton reactive API python sender specific client options """

    def __init__(self):
        """ SenderOptions cconstructor """
        super(SenderOptions, self).__init__()
        self.add_control_options()
        self.add_logging_options()
        self.add_transaction_options()
        self.add_connection_options()
        self.add_link_options()
        self.add_message_options()
        self.add_reactor_options()
        self.set_default('count', 1)
        self.set_default('duration_mode', 'after-send')
        self.get_option('--duration-mode').choices = ['before-send', 'after-send',
                                                      'after-send-tx-action']

    def add_message_options(self):
        """ add the message options """
        group = optparse.OptionGroup(self, "Message options")
        group.add_option("-i", "--msg-id", type="string",
                         help="use the supplied id instead of generating one")
        group.add_option("-S", "--msg-subject", type="string",
                         help="specify a subject")
        group.add_option("--msg-address", action="store", type="string",
                         help="message address")
        group.add_option("--msg-reply-to", type="string",
                         help="specify reply-to address")
        group.add_option("--msg-durable", action="store", type="string", default="no",
                         help="send durable messages")
        group.add_option("--msg-ttl", action="store", type="int",
                         help="message time-to-live (ms)")
        group.add_option("--msg-priority", action="store", type="int",
                         help="message priority")
        group.add_option("--msg-correlation-id", action="callback", type="string",
                         help="message correlation id",
                         callback=str_to_unicode)
        group.add_option("--msg-user-id", type="string",
                         help="message user id")
        group.add_option("--msg-group-id", type="string",
                         help="message group id")
        group.add_option("--msg-group-seq", type="int", action="store",
                         help="message group sequence")
        group.add_option("-P", "--msg-property", type="string",
                         help="specify message property ('~' enables type auto-cast)",
                         dest="msg_properties", default=[],
                         metavar="NAME=VALUE|NAME~VALUE",
                         action="callback", callback=to_unicode)
        group.add_option("-M", "--msg-content-map-item", type="string",
                         help="specify map entry for message body ('~' enables type auto-cast)",
                         dest="msg_map_items", default=[],
                         metavar="NAME=VALUE|NAME~VALUE",
                         action="callback", callback=to_unicode)
        group.add_option("-L", "--msg-content-list-item", type="string",
                         help="specify list entry for message body ('~' enables type auto-cast)",
                         dest="msg_list_items", default=[],
                         metavar="NAME|~NAME",
                         action="callback", callback=to_unicode)
        group.add_option("--msg-content-from-file", action="store", type="string",
                         help="message content loaded from file", metavar="<filename>")
        group.add_option("--msg-content", action="callback", type="string",
                         help="message content", metavar="<content>",
                         callback=str_to_unicode)
        group.add_option("--msg-content-type", action="store", type="string",
                         help="message content type", metavar="<content-type>")
        group.add_option("--content-type", type="choice",
                         help="typecast the string arguments in msg-content* (default %default)",
                         choices=['string', 'int', 'long', 'float', 'bool'])
        self.add_option_group(group)

    def add_reactor_options(self):
        """ add receiver's options  """
        group = optparse.OptionGroup(self, "Reactor options")
        group.add_option("--reactor-auto-settle-off", action="store_true",
                         help='disable auto settle mode')
        group.add_option("--reactor-peer-close-is-error", action="store_true", default=False,
                         help="report error on peer disconnect")
        self.add_option_group(group)

    def add_control_options(self):
        """ add the control options """
        super(SenderOptions, self).add_control_options()
        group = [group for group in self.option_groups if group.title == "Control Options"][0]
        group.add_option("--on-release", type="choice",
                         help="action to take when a message is released",
                         choices=["ignore", "retry", "fail"], default="ignore")


class ReceiverOptions(SRCoreOptions):
    """ Proton reactive API python receiver specific client options """

    def __init__(self):
        """ ReceiverOptions constructor """
        super(ReceiverOptions, self).__init__()
        self.add_control_options()
        self.add_logging_options()
        self.add_transaction_options()
        self.add_connection_options()
        self.add_link_options()
        self.add_receiver_options()
        self.add_reactor_options()
        self.set_default('duration_mode', 'after-receive')
        self.get_option("--duration-mode").choices = \
            ['before-receive', 'after-receive', 'after-receive-action', 'after-receive-tx-action']

    def add_control_options(self):
        """ add the control options """
        super(ReceiverOptions, self).add_control_options()
        group = [group for group in self.option_groups if group.title == "Control Options"][0]
        group.add_option("--dynamic", action="store_true",
                         help='use dynamic source')

    def add_link_options(self):
        """ add the control options """
        super(ReceiverOptions, self).add_link_options()
        group = [group for group in self.option_groups if group.title == "Link Options"][0]
        group.add_option("--link-dynamic-node-properties", action="append", default=[],
                         help="properties of dynamic node", metavar="KEY=VALUE")

    def add_receiver_options(self):
        """ add receiver's options  """
        group = optparse.OptionGroup(self, "Receiver's options")
        group.add_option("--process-reply-to", action="store_true",
                         help="send message to reply-to address if enabled "
                              "and message got reply-to address")
        group.add_option("-a", "--action", type="choice", default="acknowledge",
                         help="action on acquired message (default %default)",
                         choices=['acknowledge', 'reject', 'release', 'noack'])
        group.add_option("--action-size", type="int", default=1,
                         help="related 'action' is applied in the batch of given size "
                              "(default %default)")
        group.add_option("--recv-selector", action="callback", type="string",
                         help='consumer selector',
                         callback=str_to_unicode)
        group.add_option("--recv-browse", action="store_true",
                         help='browsing consumer')
        group.add_option("--recv-consume", action="store_true",
                         help='common consumer')
        group.add_option("--recv-filter", action="append", default=[],
                         help="consumer filter", metavar="KEY=VALUE")
        group.add_option("--recv-listen", action="store_true",
                         help='enable listener mode (p2p)')
        self.add_option_group(group)

    def add_reactor_options(self):
        """ add receiver's options  """
        group = optparse.OptionGroup(self, "Reactor options")
        group.add_option("--reactor-prefetch", type="int", default=10,
                         help="receiver's prefetch count (default %default)")
        group.add_option("--reactor-auto-accept", action="store_true", default=False,
                         help='set the auto accept on')
        group.add_option("--reactor-peer-close-is-error", action="store_true", default=False,
                         help="report error on peer disconnect")
        group.add_option("--reactor-auto-settle-off", action="store_true",
                         help='disable auto settle mode')
        self.add_option_group(group)
