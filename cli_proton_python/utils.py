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

""" Various client's common utils functions """

from __future__ import absolute_import, print_function, division

import os
import sys
import time
import logging

from cli_proton_python import formatter


if sys.version_info > (3,):
    long = int  # pylint: disable=redefined-builtin,invalid-name

def nameval(in_string):
    """
    converts given string to key, value and separator triplets

    :param in_string: key/value pair
    :type in_string: str (unicode)
    :return: key, value and separator triplet
    :rtype: tuple
    """
    idx = in_string.find("=")
    separator = '='
    if idx >= 0:
        name = in_string[0:idx]
        value = in_string[idx + 1:]
    else:
        idx = in_string.find("~")
        separator = '~'
        if idx >= 0:
            name = in_string[0:idx]
            value = in_string[idx + 1:]
        else:
            name = in_string
            value = None
    return name, value, separator

def sleep4next(in_ts, in_count, in_duration, in_indx):
    """
    custom sleep for checkpoints

    .. deprecated:: 1.0.0
       use scheduler instead
    """
    if in_duration > 0 and in_count > 0:
        cummulative_dur = 1.0 * in_indx * in_duration / in_count

        # wait only if the repeat_duration is defined and > 0
        while True:
            if time.time() - in_ts - cummulative_dur > -0.1:
                break

            time.sleep(0.1)

def hard_retype(value):
    """
    tries to converts value to relevant type by re-typing

    :param value: value to be converted
    :type value: str (unicode)
    :return: re-typed value
    :rtype: int, float, bool, str
    """
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            if value.lower() in ('true', 'false'):
                return value.lower() == 'true'
            return value

def print_message(msg, msg_format):
    """
    prints a message in coresponding format

    :param msg: message
    :type msg: proton.Message
    :param msg_format: pre-defined message format
    :type msg_format: str
    """

    if msg_format == 'body':
        print(msg.body)
    elif msg_format == 'dict':
        print(formatter.Formatter(msg).print_message_as_dict())
    elif msg_format == 'upstream':
        print(formatter.Formatter(msg).print_message())
    elif msg_format == 'interop':
        print(formatter.Formatter(msg).print_message_as_interop())
    elif msg_format == 'json':
        print(formatter.Formatter(msg).print_message_as_json())

def retype_content(content, content_type):
    """
    converts the content depending on type

    :param content: message content
    :type content: str (unicode)
    :param content_type: message content type
    :type content_type: str
    :return: re-typed content according to given content_type
    :rtype: int, float, long, bool, str
    """
    if content_type == 'int':
        return int(content)
    elif content_type == 'float':
        return float(content)
    elif content_type == 'long':
        return long(content)
    elif content_type == 'bool':
        return bool(content)
    return content

def prepare_flat_map(entries, e_type=None):
    """ prepares map content from multiple key, value pairs

    .. note:: only flat map is currently supported

    :param entries: list of key, separator, value triplets
    :type entries: list
    :param e_type: map entries desired content type (default: None)
    :type e_type: str
    :return: flat map containing given entries of given type
    :rtype: dict
    """
    flat_map = {}
    for entry in entries:
        name, val, separator = nameval(entry)
        if e_type:
            flat_map[name] = retype_content(val, e_type)
        elif separator == "~":
            flat_map[name] = hard_retype(val)
        else:
            flat_map[name] = val
    return flat_map

def set_up_client_logging(level):
    """
    sets up the client library logging

    :param level: log level number or proton logging type
    :type level: int, string
    """

    if level == 'TRANSPORT_DRV':
        # proton: Log driver related events, e.g. initialization, end of stream, etc.
        os.environ["PN_TRACE_DRV"] = "true"
    elif level == 'TRANSPORT_FRM':
        # proton: Log frames into/out of the transport.
        os.environ["PN_TRACE_FRM"] = "true"
    elif level == 'TRANSPORT_RAW':
        # proton: Log raw binary data into/out of the transport.
        os.environ["PN_TRACE_RAW"] = "true"
    else:
        # common logging
        int_level = getattr(logging, level.upper(), None)
        if not isinstance(int_level, int):
            raise ValueError('Invalid log level: %s' % level)
        logging.basicConfig(level=level.upper())

def dump_event(event):
    """
    dumps proton event object

    :param event: reactor event to be dumped
    :type event: proton.Event
    """
    statistics = {}
    event_objs = ['connection', 'context', 'delivery', 'link',
                  'reactor', 'receiver', 'sender', 'session', 'transport']
    for str_obj in event_objs:
        statistics[str_obj] = {}
        ev_obj = getattr(event, str_obj)
        for obj_attr in dir(ev_obj):
            if (not obj_attr.startswith('_') and not obj_attr.isupper()
                    and not callable(getattr(ev_obj, obj_attr))):
                statistics[str_obj][obj_attr] = '%s' % getattr(ev_obj, obj_attr)

    print(formatter.Formatter(statistics).print_stats())

def dump_error(err_message):
    """
    dump error message in parsable format

    :param err_message: error massage to be logged
    :type err_message: str
    """
    print(formatter.Formatter(err_message).print_error(), file=sys.stderr)
