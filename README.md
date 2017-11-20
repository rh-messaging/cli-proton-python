# CLI-PROTON-PYTHON
Proton Python clients is a collection of reactive messaging test clients built on [python-qpid-proton](https://pypi.python.org/pypi/python-qpid-proton) AMQP1.0 based messaging library.
cli-proton-python is a part of Unified Messaging Test Clients that offers equivalent functionality when using different programing languages or APIs.

current releated projects:
* [cli-java](https://github.com/rh-messaging/cli-java)
* [cli-rhea](https://github.com/rh-messaging/cli-rhea)
* [cli-netlite](https://github.com/rh-messaging/cli-netlite)
* [cli-proton-ruby](https://github.com/rh-messaging/cli-proton-ruby)

[![Build Status](https://travis-ci.org/rh-messaging/cli-proton-python.svg?branch=master)](https://travis-ci.org/rh-messaging/cli-proton-python)

### Installation

cli-proton-python requires [Python](https://python.org/) v2.4+ to run.

```sh
$ pip install cli-proton-python
```

### Using

Using cmd client part

```sh
$ cli-proton-python-sender --broker-url "username:password@localhost:5672/queue_test" --count 1 --msg-content "text message" --log-msgs dict
$ cli-proton-python-receiver --broker-url "username:password@localhost:5672/queue_test" --count 1 --log-msgs dict
```

Using in script or node

```python
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
```

License
----

Apache v2
