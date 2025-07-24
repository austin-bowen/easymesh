from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable
from itertools import chain, groupby
from random import Random
from typing import Any

from rosy.specs import MeshNodeSpec
from rosy.types import Service, Topic

GroupKey = Callable[[MeshNodeSpec], Any]


class TopicLoadBalancer(ABC):
    @abstractmethod
    def choose_nodes(self, nodes: list[MeshNodeSpec], topic: Topic) -> list[MeshNodeSpec]:
        """
        Takes a list of nodes listening to the given topic
        and returns which nodes to send the message to.
        """
        ...  # pragma: no cover


class ServiceLoadBalancer(ABC):
    @abstractmethod
    def choose_node(self, nodes: list[MeshNodeSpec], service: Service) -> MeshNodeSpec | None:
        """
        Takes a list of nodes providing the given service
        and returns which node to send the request to.
        """
        ...  # pragma: no cover


class NoopTopicLoadBalancer(TopicLoadBalancer):
    """No load balancing. Sends to all nodes."""

    def choose_nodes(self, nodes: list[MeshNodeSpec], topic: Topic) -> list[MeshNodeSpec]:
        return nodes


def node_name_group_key(node: MeshNodeSpec) -> str:
    return node.id.name


class GroupingTopicLoadBalancer(TopicLoadBalancer):
    """
    Groups nodes according to ``group_key`` and applies
    the given load balancer to each group.
    """

    def __init__(
            self,
            group_key: GroupKey,
            load_balancer: TopicLoadBalancer,
    ):
        self.group_key = group_key
        self.load_balancer = load_balancer

    def choose_nodes(self, nodes: list[MeshNodeSpec], topic: Topic) -> list[MeshNodeSpec]:
        if not nodes:
            return []

        nodes = sorted(nodes, key=self.group_key)
        grouped_nodes = (list(group) for _, group in groupby(nodes, key=self.group_key))
        return list(chain.from_iterable(
            self.load_balancer.choose_nodes(group, topic)
            for group in grouped_nodes
        ))


class RandomLoadBalancer(TopicLoadBalancer, ServiceLoadBalancer):
    """Chooses a single node at random."""

    def __init__(self, rng: Random = None):
        self.rng = rng or Random()

    def choose_nodes(self, nodes: list[MeshNodeSpec], topic: Topic) -> list[MeshNodeSpec]:
        return [self.rng.choice(nodes)] if nodes else []

    def choose_node(self, nodes: list[MeshNodeSpec], service: Service) -> MeshNodeSpec | None:
        return self.rng.choice(nodes) if nodes else None


class RoundRobinLoadBalancer(TopicLoadBalancer, ServiceLoadBalancer):
    """
    Chooses a single node in a round-robin fashion, based on
    the number of times a message is sent on the topic.
    """

    def __init__(self):
        self._topic_counter: dict[Topic, int] = Counter()
        self._service_counter: dict[Service, int] = Counter()

    def choose_nodes(self, nodes: list[MeshNodeSpec], topic: Topic) -> list[MeshNodeSpec]:
        if not nodes:
            return []

        i = self._topic_counter[topic] % len(nodes)
        self._topic_counter[topic] += 1
        return [nodes[i]]

    def choose_node(self, nodes: list[MeshNodeSpec], service: Service) -> MeshNodeSpec | None:
        if not nodes:
            return None

        i = self._service_counter[service] % len(nodes)
        self._service_counter[service] += 1
        return nodes[i]
