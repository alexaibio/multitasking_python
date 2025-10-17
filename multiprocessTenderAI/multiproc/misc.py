import time

from functools import partial
from pathlib import Path
from typing import List, Optional

from multiproc.pipeline import LambdaNode, Pipeline, PassNode


class BaseNodeFactory:
    """
    A factory that creates `Node` objects from a given function, layer and optionally dependencies.
    """
    def create_node(self, func, layer, dependencies=None):
        raise NotImplementedError


class NodeFactory(BaseNodeFactory):
    def __init__(self, node_args=None, node_kwargs=None):
        self.node_args = node_args or tuple()
        self.node_kwargs = node_kwargs or {}

    def create_node(self, func, layer, dependencies=None):
        if isinstance(func, str): # if func is string 'func' make it PassNode (do not execute) / otherwise LambdaNode - execute
            return PassNode(name=func, layer=layer, dependencies=dependencies)

        name = func.__name__
        func = partial(func, *self.node_args, **self.node_kwargs)
        return LambdaNode(func, name=name, layer=layer, dependencies=dependencies)


def fake_func(node_name, layer):
    print(f'Would have run node {node_name} in layer {layer}.')
    time.sleep(1.0)


class DryRunNodeFactory(BaseNodeFactory):
    def create_node(self, func, layer, dependencies=None):
        name = func.__name__
        func = partial(fake_func, name, layer)
        return LambdaNode(func, name=name, layer=layer, dependencies=dependencies)


def get_skip_nodes_from_log_dir(log_dir: Path, layer_names: Optional[List[str]] = None):
    """
    Collect the set of nodes to skip according a given pipeline execution log directory.

    Such a log directory is structured as follows:
        - <name of layer>
            - <name of node within the layer>
            - ...
        - ...

    If a file referring to a node is found, it will be marked to be skipped.

    Args:
        log_dir: Path to the log directory from which to collect nodes to skip.
        layer_names: Optional list of layers to consider for the search.

    Returns:
        List of node names which should be skipped according to the log directory.
    """
    skips = []
    for layer_dir in log_dir.iterdir():
        if layer_dir.is_dir() and (layer_names is None or layer_dir.name in layer_names):
            for file in layer_dir.iterdir():
                if file.is_file():
                    skips.append(file.name)
    return skips


def build_pipeline(pipeline_def, node_factory: BaseNodeFactory):
    """
    Builds and returns a `Pipeline` using the given pipeline definition and a node factory.

    The pipeline definition is a dictionary where layer names (as string) map to lists of node declarations.
    A node declaration is either a function object or a tuple consisting of a function object and a list of
    dependencies given as strings. The node name will be derived from the function name.

    An exemplary pipeline definition could look as follows:
    ```python
    pipeline_def = {
        'layer1': [
            foo,
            [bar, 'foo']
        ],
        'layer2': [
            baz, ['foo', 'bar']
        ]
    }
    ```

    Args:
        pipeline_def: Pipeline definition following the format described above.
        node_factory: A node factory which is used to create `Node` objects.

    Returns:
        A `Pipeline` object representing the DAG corresponding to the given pipeline definition.
    """
    nodes = []
    for layer, node_declarations in pipeline_def.items():
        for node_decl in node_declarations:
            if isinstance(node_decl, tuple):
                func, deps = node_decl
                nodes.append(node_factory.create_node(func, layer, deps))
            else:
                nodes.append(node_factory.create_node(node_decl, layer))

    return Pipeline(nodes)
