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

""" Clients output formatter module """

from __future__ import absolute_import

import sys
import json
import ast
import proton

if sys.version_info > (3,):
    # define long for Python 3.x (int and long were unified)
    long = int  # pylint: disable=redefined-builtin,invalid-name


class Formatter(object):
    """ Output formatter class for clients """

    def __init__(self, message):
        """ Formatter constructor

        :param message: message to be printed
        :type message: object
        """
        self.msg = message

    def print_message(self):
        """
        prints message in default upstream format

        :return: message to be printed in upstream format
        :rtype: str
        """
        args = []
        props = self.msg.__dict__["properties"]
        optsstring = ""
        sorted_keys = sorted(props)
        for k in sorted_keys:
            optsstring = "%s'%s': '%s', " % (optsstring, str(k), str(props[k]))
        args.append("%s=%s" % ("properties", "{" + optsstring[:-2] + "}"))

        if self.msg.body is not None:
            if args:
                args.append("content='%s'" % self.msg.body)
            else:
                args.append(repr(self.msg.body))
        else:
            args.append("content=''")
        return "Message(%s)" % (", ".join(args))

    @staticmethod
    def format_int(in_data):
        """
        formats integer value

        :param in_data: input data
        :type in_data: int, long
        :return: input data string formated as int
        :rtype: str
        """
        int_res = "%d" % in_data
        return int_res

    @staticmethod
    def format_float(in_data):
        """
        formats float value

        :param in_data: input data
        :type in_data: float
        :return: input data string formated as float
        :rtype: str
        """
        int_res = "%f" % in_data
        return int_res

    @staticmethod
    def format_dict(in_data):
        """
        formats dictionary

        :param in_data: input data
        :type in_data: dict
        :return: input data string formated as dict
        :rtype: str
        """
        int_res = '{'
        list_data = []
        for key, val in in_data.items():
            list_data.append("'%s': %s" % (key, Formatter.format_object(val)))
        int_res += ', '.join(list_data)
        int_res += '}'
        return int_res

    @staticmethod
    def format_list(in_data):
        """
        formats list

        :param in_data: input data
        :type in_data: list
        :return: input data string formated as list
        :rtype: str
        """
        int_res = '['
        list_data = []
        for val in in_data:
            list_data.append("%s" % (Formatter.format_object(val)))
        int_res += ', '.join(list_data)
        int_res += ']'
        return int_res

    @staticmethod
    def format_string(in_data):
        """
        formats string

        :param in_data: input data
        :type in_data: str, unicode, bytes
        :return: input data string formatted as string
        :rtype: str
        """
        if isinstance(in_data, bytes):
            in_data = in_data.replace(b'\0', b'\\0').decode()
        int_res = "'%s'" % (Formatter.quote_string_escape(in_data))
        return int_res

    @staticmethod
    def format_object(in_data):
        """
        formats general object

        :param in_data: input data
        :type in_data: None, bool, int, long, float, dict, list, str, unicode, bytes
        :return: input data converted to string
        :rtype: str
        """
        if in_data is None:
            return "None"
        elif isinstance(in_data, bool):
            return str(in_data)
        elif isinstance(in_data, (int, long)):
            return Formatter.format_int(in_data)
        elif isinstance(in_data, float):
            return Formatter.format_float(in_data)
        elif isinstance(in_data, dict):
            return Formatter.format_dict(in_data)
        elif isinstance(in_data, list):
            return Formatter.format_list(in_data)
        return Formatter.format_string(in_data)

    def print_message_as_dict(self):
        """
        prints message in python dictionary form

        :return: message to be printed in dictionary format
        :rtype: str
        """
        if isinstance(self.msg, proton.Message):
            int_list_keys = ['address', 'annotations', 'content', 'content_encoding',
                             'content_type', 'correlation_id', 'creation_time',
                             'delivery_count', 'durable', 'expiry_time',
                             'first_acquirer', 'group_id', 'group_sequence',
                             'id', 'inferred', 'instructions', 'priority', 'properties',
                             'reply_to', 'reply_to_group_id', 'subject', 'ttl', 'user_id']
        else:
            raise TypeError("Unable to detect message format", type(self.msg))

        int_result = "{"

        for k in int_list_keys:
            if k == 'content':
                int_value = getattr(self.msg, 'body')
            else:
                int_value = getattr(self.msg, k)

            # fix output to conform to other clients
            if k == 'expiry_time':
                k = 'expiration'  # rename field
                int_value = int(int_value * 1000)  # convert to milliseconds
            if k == 'ttl':
                int_value = int(int_value * 1000)  # convert to milliseconds

            int_result += "'%s': %s, " % (k, Formatter.format_object(int_value))
        # last comma remove
        int_result = int_result[0:-2]
        int_result += "}"
        return int_result

    def print_message_as_interop(self):
        """
        Print message in AMQP interoperable format

        :return: message to be printed in interoperable format
        :rtype: str
        """
        if isinstance(self.msg, proton.Message):
            int_list_keys = ['address', 'content', 'content_encoding',
                             'content_type', 'correlation_id', 'creation_time',
                             'delivery_count', 'durable', 'expiry_time',
                             'first_acquirer', 'group_id', 'group_sequence',
                             'id', 'priority', 'properties', 'reply_to',
                             'reply_to_group_id', 'subject', 'ttl', 'user_id']
        else:
            raise TypeError("Unable to detect message format", type(self.msg))

        int_result = "{"

        for k in int_list_keys:
            if k == 'content':
                int_value = getattr(self.msg, 'body')
            else:
                int_value = getattr(self.msg, k)

            # fix output to conform to other clients
            if k == 'expiry_time':
                k = 'absolute-expiry-time'  # rename field
                int_value = int(int_value * 1000)  # convert to milliseconds
            if k == 'ttl':
                int_value = int(int_value * 1000)  # convert to milliseconds
            if k == 'creation_time':
                int_value = int(int_value * 1000)  # convert to milliseconds
            if k == 'user_id' and (int_value is None or int_value == ''):
                int_value = None
            if (k == 'content_type'
                    and (int_value is None or int_value == '' or int_value == 'None')):
                int_value = None
            if (k == 'content_encoding'
                    and (int_value is None or int_value == '' or int_value == 'None')):
                int_value = None
            if k in ['id', 'correlation_id']:
                if int_value is not None and str(int_value).startswith("ID:"):
                    int_value = int_value[3:]

            int_result += "'%s': %s, " % (k.replace("_", "-"), Formatter.format_object(int_value))
        # last comma remove
        int_result = int_result[0:-2]
        int_result += "}"

        return int_result

    def print_message_as_json(self):
        """
        Print message in JSON form

        :return: message to be printed in JSON form
        :rtype: str
        """
        return json.dumps(ast.literal_eval(self.print_message_as_interop()))

    def print_stats(self):
        """
        print statistics information

        :return: prefix message with string indicating statistics
        :rtype: str
        """
        return "STATS %s" % self.msg

    def print_error(self):
        """
        print error information

        :return: prefix message with string indicating error
        :rtype: str
        """
        return "ERROR {'cause' :'%s'}" % self.msg

    # ------ Support formatting functions ------ #
    @staticmethod
    def quote_string_escape(in_data):
        """
        escapes quotes in given string

        :param in_data: input string
        :type in_data: str, unicode
        :return: input string with quotes escaped
        :rtype: str, unicode
        """
        return in_data.replace("'", "\\'")
