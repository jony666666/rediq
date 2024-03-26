from typing import Optional, Literal, Dict
from asyncio import iscoroutinefunction
from redis.asyncio.client import Redis

from .constants import (
    DEFAULT_TASK_PRIORITY,
    DEFAULT_TASK_RETRIES,
    DEFAULT_TASK_TIMEOUT,
    DEFAULT_TASK_QUEUE,
    DEFAULT_CRON_EXPRESSION,
    DEFAULT_CRON_RETRIES,
    DEFAULT_CRON_TIMEOUT,
)

class Rediq:

    def __init__(
        self, 
        redis_url: Optional[str] = None,
        redis_config: Optional[dict] = None,
        serializer_name: Optional[Literal['json', 'ujson', 'pickle']] = None,
    ):

        from .queue import Queue
        from .serializers import get_serializer

        redis_url = redis_url or "redis://localhost:6379/0"
        redis_config = redis_config or dict()
        serializer_name = serializer_name or 'pickle'

        self._redis = Redis.from_url(redis_url, **redis_config)
        self._serializer = get_serializer(serializer_name)

        self._queue = Queue(app = self)

        self._task_map = dict()
        self._cron_map = dict()
        self._event_map = dict()

        self._worker_started = False

    def event(self, *args,
        event_name: Optional[ Literal[
            'before_task_start', 
            'before_task_retry', 
            'after_task_success', 
            'after_task_failure',
            'before_worker_startup', 
            'after_worker_shutdown',
        ] ] = None,
    ):
        
        if self._worker_started:
            raise RuntimeError('worker is already started. cannot register event handler now')
        
        def inner(corofn):

            if not iscoroutinefunction(corofn):
                raise TypeError(f'a event handler must be a coroutine function')
            _event_name = event_name or corofn.__name__
            assert _event_name in (
                'before_task_start', 
                'before_task_retry', 
                'after_task_success', 
                'after_task_failure',
                'before_worker_startup', 
                'after_worker_shutdown',
            ), _event_name
            if _event_name in self._event_map:
                raise ValueError(f'a event handler with name "{_event_name}" already exists')
            self._event_map[_event_name] = corofn
            return corofn

        if len(args) == 1 and callable(args[0]):
            return inner(args[0])
        return inner

    def task(self, *args,
        task_name: Optional[str] = None,
        task_queue: str = DEFAULT_TASK_QUEUE,
        task_retries: int = DEFAULT_TASK_RETRIES,
        task_priority: int = DEFAULT_TASK_PRIORITY,
        task_timeout: int = DEFAULT_TASK_TIMEOUT,
    ):
        
        from .publisher import TaskPublisherFactory

        if self._worker_started:
            raise RuntimeError('worker is already started. cannot register task now')

        def inner(corofn):

            if not iscoroutinefunction(corofn):
                raise TypeError(f'a task must be a coroutine function')
            
            _task_name = task_name or corofn.__name__
            assert type(_task_name) is str and _task_name, _task_name
            assert type(task_queue) is str and task_queue, task_queue
            assert type(task_retries) is int and task_retries >= 0, task_retries
            assert type(task_priority) is int and task_priority >= 0, task_priority
            assert type(task_timeout) is int and task_timeout >= 0, task_timeout

            if _task_name in self._task_map:
                raise ValueError(f'a task with name "{_task_name}" already exists')

            self._task_map[_task_name] = dict(
                task_queue = task_queue,
                task_retries = task_retries,
                task_priority = task_priority,
                task_timeout = task_timeout,
                corofn = corofn,
            )
            
            return TaskPublisherFactory(app = self, task_name = _task_name)

        if len(args) == 1 and callable(args[0]):
            return inner(args[0])
        return inner
        
    def cron(self, *args,
        cron_name: Optional[str] = None,
        cron_expression: str = DEFAULT_CRON_EXPRESSION,
        cron_retries: int = DEFAULT_CRON_RETRIES,
        cron_timeout: int = DEFAULT_CRON_TIMEOUT,
    ):
        
        if self._worker_started:
            raise RuntimeError('worker is already started. cannot register cron job now')
        
        def inner(corofn):

            if not iscoroutinefunction(corofn):
                raise TypeError('a cron job must be a coroutine function')

            _cron_name = cron_name or corofn.__name__
            assert type(_cron_name) is str and _cron_name, _cron_name
            assert type(cron_expression) is str and cron_expression, cron_expression
            assert type(cron_retries) is int and cron_retries >= 0, cron_retries
            assert type(cron_timeout) is int and cron_timeout >= 0, cron_timeout

            if _cron_name in self._cron_map:
                raise ValueError(f'a cron job with name "{_cron_name}" already exists')
            
            self._cron_map[_cron_name] = dict(
                cron_expression = cron_expression,
                cron_retries = cron_retries,
                cron_timeout = cron_timeout,
                corofn = corofn,
                parser = CronParser(cron_expression),
            )

            return corofn
        
        if len(args) == 1 and callable(args[0]):
            return inner(args[0])
        return inner