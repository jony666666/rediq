from .app import Rediq
from .constants import (
    DEFAULT_TASK_QUEUE_SIZE
)

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

        return task_id