import time
from datetime import datetime
from pathlib import Path
from typing import Union

from multiproc.executor import PipelineObserver
from multiproc.pipeline import Pipeline, Node


class PipelineLogger(PipelineObserver):
    """
    A pipeline observer that logs started and finished nodes to the console.
    """
    def __init__(self):
        self.runtime_stats = {}

    def node_started(self, pipeline: Pipeline, node: Node):
        self.runtime_stats.setdefault(pipeline, {})[node] = time.time()

        curr_time = datetime.now().strftime('%H:%M:%S')
        print(f'{curr_time} Running step {node}')

    def node_finished(self, pipeline: Pipeline, node: Node):
        start_time = self.runtime_stats[pipeline][node]
        runtime = time.time() - start_time
        print(f'Step {node} done, time taken {runtime:8.2f} seconds.')


class PipelineBookkeeper(PipelineObserver):
    """
    A pipeline observer that logs finished nodes into a given log directory.

    Such a log directory can be used to track the execution of a pipeline over the filesystem
    or to skip running specific nodes upon next pipeline executions.

    See also `tenderai.s700_pipeline.misc.get_skip_nodes_from_log_dir`.

    Args:
        log_path: Path to the log directory to which to write node logs.
    """
    def __init__(self, log_path: Union[str, Path]):
        self.log_path = Path(log_path)

    def node_finished(self, pipeline: Pipeline, node: Node):
        file_path = self.log_path / node.layer / node.name
        file_path.touch()
