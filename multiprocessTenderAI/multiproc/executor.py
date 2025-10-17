import concurrent.futures

from concurrent.futures import ProcessPoolExecutor, FIRST_COMPLETED
from threading import Thread, Event
from functools import partial
from typing import Optional, List, Union

from multiproc.pipeline import Pipeline, Node, order_nodes


class PipelineObserver:
    """
    Base observer class for pipeline executions.
    """
    def node_started(self, pipeline: Pipeline, node: Node):
        """
        Invoked when a node of the given pipeline is executed.
        """
        pass

    def node_finished(self, pipeline: Pipeline, node: Node):
        """
        Invoked when a node of the given pipeline has finished executing.
        """
        pass


class BaseExecutor:
    """
    An executor allows the execution of a DAG defined by a Pipeline object.

    `PipelineObserver` objects can registered which are invoked during the execution accordingly.
    """
    def __init__(self):
        self.observers = []

    def execute(self,
                pipeline: Pipeline,
                target_nodes: Optional[List[Union[str, Node]]] = None,
                skip_nodes: Optional[List[Union[str, Node]]] = None):
        """
        Executes a given pipeline.

        The execution depends on the actual implementation.

        Args:
            pipeline: The pipeline to execute.
            target_nodes: Set of nodes (either `Node` objects or strings corresponding to registered nodes)
                to which the pipeline should be executed to.
                All nodes "below" these target nodes (i.e. dependencies) are executed, all nodes "above"
                (i.e. successors) are skipped.
            skip_nodes: Set of nodes (either `Node` objects or strings corresponding to registered nodes)
                which should be skipped.

        """
        target_nodes = Node.resolve_nodes(target_nodes) if target_nodes else pipeline.nodes
        skip_nodes = Node.resolve_nodes(skip_nodes) if skip_nodes else []

        self._execute(pipeline, target_nodes, skip_nodes)

    def _execute(self,
                 pipeline: Pipeline,
                 target_nodes: List[Node],
                 skip_nodes: List[Node]):
        raise NotImplementedError

    def register_execution_observer(self, observer: PipelineObserver):
        self.observers.append(observer)

    def _node_started_callback(self, pipeline: Pipeline, node: Node):
        for observer in self.observers:
            observer.node_started(pipeline, node)

    def _node_finished_callback(self, pipeline: Pipeline, node: Node):
        for observer in self.observers:
            observer.node_finished(pipeline, node)


class SequentialExecutor(BaseExecutor):
    """
    Implementation of a sequential pipeline executor.

    Nodes are guaranteed to be invoked sequentially according to their dependency relations and to run
    in the same thread, respectively process from which the execution was invoked.
    """
    def _execute(self,
                 pipeline: Pipeline,
                 target_nodes: List[Node],
                 skip_nodes: List[Node]):
        todo = order_nodes(target_nodes)

        for node in todo:
            if node in skip_nodes:
                continue
            self._node_started_callback(pipeline, node)
            node()
            self._node_finished_callback(pipeline, node)


class ParallelExecutor(BaseExecutor):
    """
    Implementation of a multi-processed parallel pipeline executor.

    Nodes that can be executed independently according to their dependency relations are run in parallel,
    which allows to run up to max_workers nodes concurrently.

    Args:
        max_workers: Number of concurrent workers (robotics_control_loop) that will execute nodes.
        log_stats_interval: The interval (in seconds) between printing the current execution stats.
            If None, no stats will be printed.
    """
    def __init__(self, max_workers=16, log_stats_interval: Optional[float] = 30.0):
        super().__init__()
        self.max_workers = max_workers
        self.log_stats_interval = log_stats_interval

    def _wrap_node(self, pipeline, node):
        self._node_started_callback(pipeline, node)
        node()
        self._node_finished_callback(pipeline, node)

    def _execute(self,
                 pipeline: Pipeline,
                 target_nodes: List[Node],
                 skip_nodes: List[Node]):
        skip_nodes = set(skip_nodes)
        done_nodes = set(skip_nodes)
        todo_nodes = set(order_nodes(target_nodes))

        num_tasks = len(todo_nodes - skip_nodes)

        futures = {}
        pending = {}

        def log_stats(cancel_event, log_interval):
            if not log_interval:
                return

            while not cancel_event.wait(timeout=log_interval):
                current_futures = list(futures.keys())
                running = [f for f in current_futures if f.running()]
                done = [f for f in current_futures if f.done()]
                print(f'Currently executing {len(running)} node(s) in parallel ({len(done)}/{num_tasks} done)')
                if len(done) != len(current_futures):
                    return

        cancel_logging = Event()
        log_thread = Thread(target=log_stats, kwargs=dict(cancel_event=cancel_logging,
                                                          log_interval=self.log_stats_interval))
        log_thread.start()

        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                while todo_nodes:
                    ready = {node for node in todo_nodes if set(node.dependencies) <= done_nodes}

                    for node in ready:
                        if node in skip_nodes:
                            print(f'Skipping node {node}')
                            done_nodes.add(node)
                            continue
                        print(f'Submitting node {node}')
                        future = executor.submit(partial(self._wrap_node, pipeline, node))
                        futures[future] = pending[future] = node

                    if not todo_nodes and not pending:
                        break

                    todo_nodes -= ready

                    done, not_done = concurrent.futures.wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        node = futures[future]
                        try:
                            future.result()
                        except Exception as e:
                            print(f'Exception occurred while processing node {node}. Cancelling remaining nodes.')
                            for fut in futures:
                                fut.cancel()
                            raise e
                        done_nodes.add(node)
                        del pending[future]
        except KeyboardInterrupt:
            print('Cancelling all futures. Press ^C again to force shutdown.')
            for future in pending.keys():
                future.cancel()
        finally:
            cancel_logging.set()
            log_thread.join()
