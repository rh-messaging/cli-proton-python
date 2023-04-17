#################
CLI-PROTON-PYTHON
#################

.. image:: https://badge.fury.io/py/cli-proton-python.svg
    :target: https://badge.fury.io/py/cli-proton-python

Proton Python clients is a collection of reactive messaging test clients built on python-qpid-proton_ AMQP1.0 based messaging library. cli-proton-python is a part of Unified Messaging Test Clients that offers equivalent functionality when using different programing languages or APIs.

current related projects:

* cli-cpp_
* cli-java_
* cli-rhea_
* cli-proton-dotnet_

************
Installation
************

cli-proton-python requires Python_ v3.8+ to run.

::

  $ pip install cli-proton-python

*****
Using
*****

Using the command line clients (please refer to --help to discover the available options)

::

    $ cli-proton-python-sender --broker-url "username:password@localhost:5672/queue_test" --count 1 --msg-content "text message" --log-msgs dict
    $ cli-proton-python-receiver --broker-url "username:password@localhost:5672/queue_test" --count 1 --log-msgs dict


Using in script

.. code-block:: python

    import proton
    from cli_proton_python import sender

    parser = sender.options.SenderOptions()

    opts, _ = parser.parse_args()
    opts.broker_url = 'username:password@localhost:5672/examples'
    opts.count = 1
    opts.msg_content = 'text message'
    opts.log_msgs = 'dict'

    container = proton.reactor.Container(sender.Send(opts))
    container.run()

*************
Documentation
*************

.. image:: https://readthedocs.org/projects/cli-proton-python/badge/?version=latest
    :target: http://cli-proton-python.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Documentation may be found on readthedocs.io: `read the docummentation`_

*******
License
*******

.. image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0

.. _Python: https://python.org/
.. _python-qpid-proton: https://pypi.python.org/pypi/python-qpid-proton
.. _cli-cpp: https://github.com/rh-messaging/cli-cpp
.. _cli-java: https://github.com/rh-messaging/cli-java
.. _cli-rhea: https://github.com/rh-messaging/cli-rhea
.. _cli-proton-dotnet: https://github.com/rh-messaging/cli-proton-dotnet
.. _read the docummentation: http://cli-proton-python.readthedocs.io/en/latest/

