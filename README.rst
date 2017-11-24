#################
CLI-PROTON-PYTHON
#################

Proton Python clients is a collection of reactive messaging test clients built on python-qpid-proton_ AMQP1.0 based messaging library. cli-proton-python is a part of Unified Messaging Test Clients that offers equivalent functionality when using different programing languages or APIs.

current related projects:

* cli-java_
* cli-rhea_
* cli-netlite_
* cli-proton-ruby_

.. image:: https://travis-ci.org/rh-messaging/cli-proton-python.svg?branch=master
    :target: https://travis-ci.org/rh-messaging/cli-proton-python

************
Installation
************

cli-proton-python requires Python_ v2.6+ to run.

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

*******
License
*******

.. image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0

.. _Python: https://python.org/
.. _python-qpid-proton: https://pypi.python.org/pypi/python-qpid-proton
.. _cli-java: https://github.com/rh-messaging/cli-java
.. _cli-rhea: https://github.com/rh-messaging/cli-rhea
.. _cli-netlite: https://github.com/rh-messaging/cli-netlite
.. _cli-proton-ruby: https://github.com/rh-messaging/cli-proton-ruby

