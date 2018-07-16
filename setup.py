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

"""A setuptools based setup module.

See:
* https://packaging.python.org/en/latest/distributing.html
* https://github.com/pypa/sampleproject
"""

from __future__ import absolute_import

from os import path
from codecs import open as c_open
from setuptools import setup, find_packages


# Get the long description from the README file
CWD = path.abspath(path.dirname('README.rst'))
with c_open(path.join(CWD, 'README.rst'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='cli-proton-python',
    # Versions should comply with PEP440.
    version='1.0.4',
    description="AMQP 1.0 Python Proton reactive clients set(sender, receiver, connector)",
    long_description=LONG_DESCRIPTION,
    url='https://github.com/rh-messaging/cli-proton-python',
    author='Petr Matousek',
    author_email='pematous@redhat.com',
    license='Apache License 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Quality Assurance',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='unified messaging clients sender receiver amqp',
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=['python-qpid-proton'],
    entry_points={
        'console_scripts': [
            'cli-proton-python-sender=cli_proton_python.sender:main',
            'cli-proton-python-receiver=cli_proton_python.receiver:main',
            'cli-proton-python-connector=cli_proton_python.connector:main',
        ],
    },
)
