# easymesh

Simple and fast inter-process message passing for distributed Python processes. Inspired by [ROS (Robot Operating System)](https://www.ros.org/).

`easymesh` allows sending messages between nodes in two different ways:
1. **Topics**: Unidirectional, "fire and forget" messages that are sent from a node to all nodes listening to that topic.
2. **Services**: Bidirectional, request-response messages that allow a node to get a response from any node hosting the service being called.

Messages can contain any Python data that is serializable by `pickle` (default) or `msgpack`. Alternatively, you can even provide your own custom serde.

Nodes can run on a single machine, or be distributed across multiple machines on a network. As long as they can talk to the coordinator node, they can figure out how to talk to each other. Nodes will automatically reconnect to the coordinator and other nodes if they lose connection.

`easymesh` also has simple load balancing: if multiple nodes of the same name are listening to a topic, then messages will be sent to them in a round-robin fashion. (The load balancing strategy can be changed or disabled if desired.)

## Show me the code!

Here are some simplified examples. See the linked files for the full code.

### Example: Sending messages using Topics

[easymesh/demo/**sender.py**](src/easymesh/demo/sender.py):
```python
import easymesh

async def main():
    node = await easymesh.build_node(name='sender')
    await node.send('some-topic', 'hello', name='world')
```

[easymesh/demo/**receiver.py**](src/easymesh/demo/receiver.py):
```python
import easymesh

async def main():
    node = await easymesh.build_node(name='receiver')
    await node.listen('some-topic', callback)
    await node.forever()

async def callback(topic, message, name=None):
    print(f'receiver got "{message} {name}" on topic={topic}')
```

**Terminal:**

```bash
$ easymesh &  # Start the coordinator node
$ python -m easymesh.demo.receiver &
$ python -m easymesh.demo.sender
receiver got "hello world" on topic=some-topic
```

### Example: Calling Services

**Client:**
```python
import easymesh

async def main():
    node = await easymesh.build_node(name='client')
    response = await node.call('multiply', 2, 2)
    assert response == 4
```

**Server:**
```python
import easymesh

async def main():
    node = await easymesh.build_node(name='server')
    await node.add_service('multiply', multiply)

async def multiply(a, b):
    return a * b
```

## Installation

```bash
pip install git+https://github.com/austin-bowen/easymesh.git
```

## Commands

Use the `--help` arg on any command to see all options.

### `$ easymesh`

Start the coordinator node. By default, it will listen on port `6374` on all interfaces.

### `$ meshlaunch [config]`

Launch a coordinator and several nodes all at once. Based on the [`roslaunch` ROS command line tool](https://wiki.ros.org/roslaunch). `config` defaults to `launch.yaml`. Check out the [template `launch.yaml`](launch.yaml) for all options, or the [demo `launch.yaml`](src/easymesh/demo/launch.yaml) for a runnable example.

### `$ meshbag {record,play,info}`

Tool for recording and playing back messages. Based on the [`rosbag` ROS command line tool](https://wiki.ros.org/rosbag). The options are:

- `record <topics>`: Record messages on the given topic(s) to a file. By default, a file named `record_<datetime>.bag` will be created in the current directory.
- `play`: Play back messages from a bag file, with the same timing between messages as when they were recorded. By default, the most recent bag file in the current directory will be played back.
- `info`: Print information about a bag file. By default, the most recent bag file in the current directory will be used.

Use the `--help` arg on any of those sub-commands to see all options.

### `$ python -m easymesh.demo.{sender,receiver}`

Example sender and receiver nodes. These are also useful for sanity checking and testing your own nodes.

### `$ python -m easymesh.demo.speedtest {send,recv}`

Performs a speed test sending and receiving messages.

Some results:

| Hardware      | Message size | Messages/s | Latency (ms) | Bandwidth (MB/s) |
|---------------|--------------|------------|--------------|------------------|
| Laptop*       | 0            | 69000      | 0.032        | N/A              |
| Laptop*       | 1 kB         | 67000      | 0.037        | 67               |
| Laptop*       | 1 MB         | 1600       | 1.1          | 1600             |
| Jetson Nano** | 0            | 6500       | 0.43         | N/A              |
| Jetson Nano** | 1 kB         | 6300       | 0.45         | 6.3              |
| Jetson Nano** | 1 MB         | 230        | 6.3          | 230              |

\* Dell XPS 17 9730 with a 13th Gen Intel Core i9-13900H CPU and 64 GB DDR5 RAM running Ubuntu 24.04 and Python 3.10.\
\** [NVIDIA Jetson Nano](https://developer.nvidia.com/embedded/jetson-nano) running Ubuntu 18.04 and Python 3.12.

## What is a mesh?

A mesh is a collection of "nodes" that can send messages to each other. A message can be any Python object. There is one node per Python process, with nodes potentially distributed across multiple machines. Each node listens to specific message "topics", and calls listener callbacks when messages are received on those topics. Each node can send messages to any topic, and the message will be sent to all listening nodes.

### How does it work?

A special "coordinator" node maintains the current mesh topology, and makes sure all nodes in the mesh know about each other. The mesh topology is a list of nodes in the mesh, their connection details, and topics they are listening to. When a new node is created, it registers itself with the coordinator, which then adds it to the mesh topology; when a node disconnects, it is removed from the mesh topology. When any change is made to the mesh topology, the coordinator node broadcasts the new mesh topology to all nodes on the mesh.

When a node needs to send a message, it uses the mesh topology to find all currently listening nodes, connects to them, and sends the message.

### Guarantees

`easymesh` only guarantees that messages will be received in the order in which they were sent from a *single* node. It is possible for messages sent from different nodes to be received out of order.

It does **not** guarantee message delivery; there are no delivery confirmations, and if a message fails to be sent to a node (e.g. due to network failure), it will not be retried.

### Security

Security is not a primary concern of `easymesh`. Messages are sent in plaintext (unencrypted) for speed, and by default, there is no authentication of nodes on the mesh.

There is optional authentication support to ensure all nodes on the mesh are allowed to be there. This is done using symmetric HMAC challenge-response. The coordinator will authenticate all nodes before adding them to the mesh, and all nodes will authenticate each other before connecting. This could come in handy when e.g. running multiple meshes on the same network, to avoid accidentally connecting a node to the wrong mesh.

Simply provide the `--authkey=...` argument when starting the coordinator, and ensure the `authkey=b'...'` argument is provided to `build_node(...)`, e.g.

```bash
$ easymesh --authkey my-secret-key
```

```python
node = await easymesh.build_node(name='my-node', authkey=b'my-secret-key')
```
