from typing import Tuple

from .app import Rediq
from .loggers import get_logger
from .contexts import (
    TaskInfoNamedTuple,
    cv_task_info,
    cv_task_retried,
    cv_task_groups,
)
from .constants import (
    DEFAULT_TASK_QUEUE_SIZE
)

logger = get_logger('publisher')

async def _execute_task_event(
    app: Rediq,
    task_name: str,
    task_groups: Tuple[str, ...],
    task_id: bytes,
):

    task_info = TaskInfoNamedTuple(
        task_id = task_id,
        task_name = task_name,
        task_queue = app._task_map[task_name]['task_queue'],
        task_retries = app._task_map[task_name]['task_retries'],
        task_timeout = app._task_map[task_name]['task_timeout'],
        task_priority = app._task_map[task_name]['task_priority'],
    )

    cv_task_info_token = cv_task_info.set(task_info)
    cv_task_retried_token = cv_task_retried.set(0)
    cv_task_groups_token = cv_task_groups.set(task_groups)

    logger.info(f'task "{task_name}" is enqueued')
    
    try:
        after_task_enqueue = app._event_map.get('after_task_enqueue')
        after_task_enqueue and await after_task_enqueue()
    except:
        logger.error('event handler "after_task_enqueue" raised an exception', exc_info=True)

    cv_task_info.reset(cv_task_info_token)
    cv_task_retried.reset(cv_task_retried_token)
    cv_task_groups.reset(cv_task_groups_token)
    
class TaskPublisherFactory:

    def __init__(self, app: Rediq, task_name: str):
        self._app = app
        self._task_name = task_name
    
    def __call__(self, *task_args, **task_kwargs):
        return TaskPublisher(self._app, self._task_name, task_args, task_kwargs)

class TaskPublisher:

    def __init__(
        self, 
        app: Rediq, 
        task_name: str,
        task_args: tuple, 
        task_kwargs: dict,
    ):
        self._app = app
        self._task_name = task_name 
        self._task_args = task_args
        self._task_kwargs = task_kwargs
        self._task_groups = ()

    def groups(self, *task_groups: str):
        instance = self.__class__(
            app = self._app,
            task_name = self._task_name,
            task_args = self._task_args,
            task_kwargs = self._task_kwargs,
        )
        instance._task_groups = task_groups
        return instance

    async def enqueue(self):

        task_queue = self._app._task_map[self._task_name]['task_queue']
        task_priority = self._app._task_map[self._task_name]['task_priority']
        
        task_message = self._app._serializer.dumps([
            self._task_name,
            self._task_args,
            self._task_kwargs,
            self._task_groups,
        ])

        task_id = await self._app._queue.enqueue(
            queue_name = task_queue,
            queue_size = DEFAULT_TASK_QUEUE_SIZE,
            priority = task_priority,
            message = task_message,
        )

        await _execute_task_event(
            app = self._app, 
            task_name = self._task_name, 
            task_groups = self._task_groups,
            task_id = task_id,
        )

        return task_id