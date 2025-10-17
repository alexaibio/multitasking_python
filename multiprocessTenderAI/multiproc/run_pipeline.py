import warnings
import argparse

from pathlib import Path
from datetime import datetime

from multiproc.pipeline_def import pipeline_def
from multiproc.pipeline import Node, order_nodes
from multiproc.misc import build_pipeline, get_skip_nodes_from_log_dir, NodeFactory, DryRunNodeFactory


from multiproc.executor import ParallelExecutor, SequentialExecutor
from multiproc.observers import PipelineLogger, PipelineBookkeeper


def run_pipeline(log_path,
                 pipeline,
                 executor,
                 target_nodes=None,
                 skip_nodes=None):
    log_path = Path(log_path)

    print(f'Running pipeline. Logging to {str(log_path)}.')

    target_nodes = order_nodes(target_nodes) if target_nodes else pipeline.nodes
    skip_nodes = Node.resolve_nodes(skip_nodes) if skip_nodes else []

    skip_layers = []
    for layer in pipeline_def.keys():
        layer_path = log_path / layer
        layer_path.mkdir(parents=True, exist_ok=True)

        fail_file = log_path / f'_{layer}_FAILURE'
        success_file = log_path / f'_{layer}_SUCCESS'

        if not fail_file.exists() and success_file.exists():
            print(f'Will skip {layer} layer as already executed successfully this month.'
                  f'If needed to re-run, remove files from {log_path}')
            skip_layers.append(layer)

    for node in pipeline.nodes:
        if node.layer in skip_layers:
            skip_nodes.append(node)

    print(f'Will skip following nodes: {skip_nodes}')

    executor.register_execution_observer(PipelineLogger())
    executor.register_execution_observer(PipelineBookkeeper(log_path))
    executor.execute(pipeline, target_nodes, skip_nodes)


def create_pipeline_log_dir():
    date = datetime.today().strftime('%m-%Y')

    log_dir = Path(config['LOG']['APP_DIR']) / f'pipeline_logs_{date}'
    log_dir.mkdir(parents=True, exist_ok=True)

    return log_dir


def main(skip_layers=None,
         skip_nodes=None,
         run_only_layers=None,
         run_only_nodes=None,
         target_layers=None,
         target_nodes=None,
         ignore_log=False,
         dry=False,
         no_parallel=False):

    if target_nodes and target_layers:
        raise ValueError(f'target_nodes and target_layers arguments are mutually exclusive')

    if run_only_nodes and run_only_layers:
        raise ValueError(f'run_only_nodes and run_only_layers arguments are mutually exclusive')

    if dry:
        node_factory = DryRunNodeFactory()
    else:
        node_factory = NodeFactory(node_kwargs=dict(increment_output_version=True, write_output=True))

    pipeline = build_pipeline(pipeline_def, node_factory)

    skip_pipeline_nodes = set()
    for node in pipeline.nodes:
        if (skip_layers and node.layer in skip_layers) or \
                (run_only_layers and node.layer not in run_only_layers) or \
                (run_only_nodes and node.name not in run_only_nodes) or \
                (skip_nodes and node.name in skip_nodes):
            skip_pipeline_nodes.add(node)

    if not ignore_log:
        skips_from_log = set(Node.resolve_nodes(get_skip_nodes_from_log_dir(log_path, list(pipeline_def.keys()))))
        skip_pipeline_nodes.update(skips_from_log)

    target_pipeline_nodes = set()
    for node in pipeline.nodes:
        if (target_layers and node.layer in target_layers) or \
                (target_nodes and node.name in target_nodes) or \
                (run_only_layers and node.layer in run_only_layers):
            target_pipeline_nodes.add(node)

    if not target_pipeline_nodes:
        target_pipeline_nodes = None

    if no_parallel:
        executor = SequentialExecutor()
    else:
        executor = ParallelExecutor(max_workers=16)

    run_pipeline(log_path, pipeline, executor, target_nodes=target_pipeline_nodes, skip_nodes=skip_pipeline_nodes)


if __name__ == '__main__':
    layers = list(pipeline_def.keys())

    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-layers', nargs='*', metavar='LAYER', choices=layers,
                        help='Skip all nodes belonging to the given layers.')
    parser.add_argument('--skip-nodes', nargs='*', metavar='NODE',
                        help='Skip given nodes.')
    parser.add_argument('--run-only-layers', nargs='*', metavar='LAYER', choices=layers,
                        help='Only run nodes belonging to the given layers. All non-matching nodes are skipped. '
                             'This option is mutually exclusive with the --run-only-nodes option.')
    parser.add_argument('--run-only-nodes', nargs='*', metavar='NODE',
                        help='Only run the given nodes. All non-matching nodes are skipped. '
                             'This option is mutually exclusive with the --run-only-layers option.')
    parser.add_argument('--target-layers', nargs='*', choices=layers, metavar='TARGET',
                        help='Use nodes in the given layers are target nodes. When target nodes are passed, the '
                             'pipeline is executed up to these nodes, including all non-skipped dependencies. '
                             'This option is mutually exclusive with the --target-nodes option.')
    parser.add_argument('--target-nodes', nargs='*', metavar='NODE',
                        help='Use the given nodes as target nodes. When target nodes are passed, the '
                             'pipeline is executed up to these nodes, including all non-skipped dependencies. '
                             'This option is mutually exclusive with the --target-layers option.')
    parser.add_argument('--ignore-log', action='store_true',
                        help='Ignore the log and do not skip any nodes by default. The --skip-* options '
                             'are not affected by this flag.')
    parser.add_argument('--dry', action='store_true',
                        help='Don\'t run the actual pipeline but only print which nodes would have ran.')
    parser.add_argument('--no-parallel', action='store_true',
                        help='Do not run the pipeline using a parallel (multi-processed) executor. '
                             'This might be useful for debugging purposes.')
    args = parser.parse_args()

    main(**vars(args))
