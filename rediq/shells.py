import click

from uuid import uuid4
from typing import Tuple, Literal, Optional

from .constants import (
    DEFAULT_TASK_QUEUE,
    DEFAULT_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_TASK_CONCURRENCY,
    DEFAULT_EXECUTOR_POOL_SIZE,
)

@click.command('rediq')
@click.argument('app-name', type=str, required=True)
@click.option('--queue-name', type=str, multiple=True, required=False)
@click.option('--skip-default-queue', is_flag=True, required=False, default=False)
@click.option('--worker-name', type=str, required=False)
@click.option('--worker-queue-size', type=int, required=False, show_default=True, default=DEFAULT_WORKER_QUEUE_SIZE)
@click.option('--worker-task-concurrency', type=int, required=False, show_default=True, default=DEFAULT_WORKER_TASK_CONCURRENCY)
@click.option('--executor-pool-name', type=click.Choice(['THREAD', 'PROCESS', 'DEFAULT'], case_sensitive=False), show_default=True, default='DEFAULT')
@click.option('--executor-pool-size', type=int, required=False, show_default=True, default=DEFAULT_EXECUTOR_POOL_SIZE)
@click.option('--log-level', type=click.Choice(['INFO', 'DEBUG', 'ERROR', 'CRITICAL', 'WARN', 'WARNING'], case_sensitive=False), required=False, show_default=True, default='INFO')
@click.option('--log-file', type=str, required=False)
def rediq_cli(
    *, 
    app_name: str, 
    queue_name: Tuple[str, ...], 
    skip_default_queue: bool, 
    worker_name: Optional[str],
    worker_queue_size: int,
    worker_task_concurrency: int,
    executor_pool_name: Literal['THREAD', 'PROCESS', 'DEFAULT'],
    executor_pool_size: int,
    log_level: Literal['INFO', 'DEBUG', 'ERROR', 'CRITICAL', 'WARN', 'WARNING'],
    log_file: Optional[str],
):

    from .app import Rediq
    from .utils import import_from_name
    from .loggers import config_logger
    from .worker import start_worker

    app = import_from_name(app_name)

    assert isinstance(app, Rediq), app

    queue_names = set(queue_name)
    skip_default_queue or queue_names.add(DEFAULT_TASK_QUEUE)
    worker_name = worker_name or f'worker-{uuid4().hex}'

    config_logger(
        level = log_level,
        prefix = worker_name,
        echo = True,
        path = log_file
    )

    start_worker(
        app,
        *queue_names,
        worker_name = worker_name,
        worker_queue_size = worker_queue_size,
        worker_task_concurrency = worker_task_concurrency,
        executor_pool_name = executor_pool_name,
        executor_pool_size = executor_pool_size,
    )