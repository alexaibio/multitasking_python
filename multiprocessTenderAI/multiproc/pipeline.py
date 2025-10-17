import time
from typing import List, Union, Optional, Callable, Set
from multiproc.errors import IntegrityError


class Node:
    """
    A node in the graph, representing a execution step in the pipeline.

    Nodes can have several dependencies, respectively parents.

    Args:
        name: The (globally) unique name of the node.
        layer: Name of the layer to which the node should correspond.
        dependencies: List of dependencies / parents for the node.
            Can either be Node objects or node names represented as strings.
            Optional.

    Raises:
        AssertionError: If a node with the given name already exists.
        TypeError: If the given node dependencies do not correspond to strings referring to nodes or Node objects.
        KeyError: If a dependency given as string does not correspond to a registered node.
    """
    registered_nodes_by_name = {}

    def __init__(self, name: str, layer: str, dependencies: Optional[List[Union[str, 'Node']]] = None):
        self.name = name
        self.layer = layer
        self.dependencies = self.resolve_nodes(dependencies) if dependencies else []
        self._register_node(self)

    @classmethod
    def _register_node(cls, node: 'Node'):
        if node.name in cls.registered_nodes_by_name:
            raise AssertionError(f'Node with name {node.name} already registered')
        cls.registered_nodes_by_name[node.name] = node

    @classmethod
    def get_node_by_name(cls, name: str) -> 'Node':
        """
        Returns a registered node given the node name.

        Args:
            name: Name of a previously registered node.

        Returns:
            Node: Node object registered by the given name.

        Raises:
            KeyError: If a node with the given name does not exist.
        """
        node = cls.registered_nodes_by_name.get(name)
        if not node:
            raise KeyError(f'No node with name {name} registered')
        return node

    @classmethod
    def get_registered_nodes(cls) -> List['Node']:
        return list(cls.registered_nodes_by_name.values())

    @classmethod
    def resolve_nodes(cls, nodes: List[Union[str, 'Node']]):
        resolved = []
        for node in nodes:
            if isinstance(node, str):
                node = cls.get_node_by_name(node)
            else:
                if not isinstance(node, cls):
                    raise TypeError(f'Given nodes must either be strings referring to nodes or Node objects.')
            resolved.append(node)
        return resolved

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def __repr__(self):
        return f'{self.layer}/{self.name}'


class DummyNode(Node):
    """
    A simple dummy node. Upon execution, it prints its name to the console and waits for 3 seconds.
    """
    def __call__(self, *args, **kwargs):
        print(f'DummyNode {self} executing')
        time.sleep(3.0)

    def __repr__(self):
        return f'DummyNode(name={self.name})'


class PassNode(Node):
    """
    A node that does nothing. Can be used for grouping certain dependencies.
    """
    def __call__(self, *args, **kwargs):
        pass

    def __repr__(self):
        return f'PassNode(name={self.name})'


class LambdaNode(Node):
    """
    A node executing a given function.

    Args:
        function: The function to execute upon execution of the node.
        kwargs: Additional arguments that are passed to the base Node class.
    """
    def __init__(self,
                 function: Callable,
                 **kwargs):
        super().__init__(**kwargs)
        self.function = function

    def __call__(self, *args, **kwargs):
        self.function(*args, **kwargs)


def order_nodes(nodes: List[Node]):
    """
    Returns the given nodes ordered by a depth-first-search.

    Returns:
        Given nodes ordered by their dependency relations.
    """
    ordered = []

    def dfs(node):
        for dep in node.dependencies:
            if dep in ordered:
                continue
            dfs(dep)
        ordered.append(node)

    for node in nodes:
        dfs(node)

    return ordered


class Pipeline:
    """
    A Pipeline represents the DAG that is formed by certain nodes and its dependencies.

    The formed graph is checked for integrity, e.g. cycles or missing start nodes initially.

    Args:
        nodes: List of nodes that belong to the pipeline.

    Raises:
        IntegrityError: When the graph formed by the given nodes contains a cycle or has no start nodes,
            i.e. nodes without incoming dependencies.

    """
    def __init__(self, nodes: List[Node]):
        self.nodes = order_nodes(nodes)
        self.check_integrity()

    @property
    def start_nodes(self):
        """
        The set of nodes with no dependencies to other nodes.
        These nodes will be executed first when the pipeline is run end-to-end.
        """
        return {node for node in self.nodes if not node.dependencies}

    def check_integrity(self):
        start_nodes = self.start_nodes
        if not start_nodes:
            raise IntegrityError(f'No start nodes (nodes with no incoming dependencies) found. Pipeline is invalid.')

        visited = set()
        finished = set()

        def dfs(node):
            if node in finished:
                return
            if node in visited:
                raise Exception(f'Cycle containing node {node} found. Pipeline is invalid')
            visited.add(node)
            for child in self.nodes:
                if node in child.dependencies:
                    dfs(child)
            finished.add(node)

        for node in self.nodes:
            dfs(node.name)
