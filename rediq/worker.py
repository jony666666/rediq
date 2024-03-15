import asyncio

from typing import Literal
from signal import SIGINT, SIGTERM, SIGHUP, Signals
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial

from .app import Rediq
from .loggers import get_logger
from .exceptions import RedisQueueEmptyError
from .contexts import (
    cv_task_info, 
    cv_task_retried,
    cv_task_result,
    cv_task_exception,
    cv_executor_pool,
    TaskInfoNamedTuple,
)
from .constants import (
    QUEUE_INTERVAL,
)

logger = get_logger('worker')

async def _execute_task(
    app: Rediq,
    task_name: str,
    task_id: bytes,
    *task_args,
    **task_kwargs,
):
    
    task_info = TaskInfoNamedTuple(
        task_id = task_id,
        task_name = task_name,
        task_queue = app._task_map[task_name]['task_queue'],
        task_retries = app._task_map[task_name]['task_retries'],
        task_timeout = app._task_map[task_name]['task_timeout'],
        task_priority = app._task_map[task_name]['task_priority'],
    )

    task_corofn = app._task_map[task_name]['corofn']

    before_task_start = app._event_map.get('before_task_start')
    before_task_retry = app._event_map.get('before_task_retry')
    after_task_success = app._event_map.get('after_task_success')
    after_task_failure = app._event_map.get('after_task_failure')

    cv_task_info_token = None
    cv_task_retried_token = None
    cv_task_result_token = None
    cv_task_exception_token = None

    logger.info(f'task "{task_name}" is starting')

    cv_task_info_token = cv_task_info.set(task_info)
    cv_task_retried_token = cv_task_retried.set(0)

    try:
        before_task_start and await before_task_start()
    except:
        logger.error('event handler "before_task_start" raised an exception', exc_info=True)

    for task_retried in range(task_info.task_retries + 1):

        if task_retried > 0:
            cv_task_retried_token = cv_task_retried.set(task_retried)
            logger.warn(f'task "{task_name}" is being retried for the {task_retried} time')
            try:
                before_task_retry and await before_task_retry()
            except Exception:
                logger.error('event handler "before_task_retry" raised an exception', exc_info=True)
        try:
            result = await asyncio.wait_for(
                fut = task_corofn(*task_args, **task_kwargs),
                timeout = task_info.task_timeout / 1000,
            )
            logger.info(f'task "{task_name}" is done')
            cv_task_result_token = cv_task_result.set(result)
            try:
                after_task_success and await after_task_success()
            except:
                logger.error('event handler "after_task_success" raised an exception', exc_info=True)
            break

        except Exception as e:
            if task_retried < task_info.task_retries:
                logger.warn(f'task "{task_name}" raised an exception and will be retried', exc_info=True)
            else:
                logger.error(f'task "{task_name}" is failed', exc_info=True)
                cv_task_exception_token = cv_task_exception.set(e)
                try:
                    after_task_failure and await after_task_failure()
                except:
                    logger.error('event handler "after_task_failure" raised an exception', exc_info=True)
    
    if cv_task_info_token:
        cv_task_info.reset(cv_task_info_token)
    if cv_task_retried_token:
        cv_task_retried.reset(cv_task_retried_token)
    if cv_task_result_token:
        cv_task_result.reset(cv_task_result_token)
    if cv_task_exception_token:
        cv_task_exception.reset(cv_task_exception_token)


def start_worker(
    app: Rediq,
    *queue_names: str,
    worker_name: str,
    worker_queue_size: int,
    worker_task_concurrency: int,
    executor_pool_name: Literal[ 'THREAD', 'PROCESS', 'DEFAULT' ],
    executor_pool_size: int,
):

    async def dequeue_loop():

        async def inner_dequeue_loop(queue_name: str):

            while True:
                if loop_ctx['shutting_down']:
                    break
                try:
                    items = await app._queue.dequeue(queue_name, QUEUE_INTERVAL)
                except RedisQueueEmptyError:
                    continue
                await worker_queue.put(( 0, *items )) # 这是基于heapq的优先队列，这里利用了tuple的排序规则

        logger.info(f'dequeue loop is starting up')
        await asyncio.gather(*( inner_dequeue_loop(s) for s in queue_names ))
        await asyncio.gather(*( worker_queue.put(( 1, )) for _ in range(worker_task_concurrency) )) # 关闭executor_loop的指令，需要排在优先队列最末端
        logger.info(f'dequeue loop is stopped')
    
    async def execute_loop():

        async def inner_execute_loop():

            while True:
                code, *items = await worker_queue.get()
                if code == 1:
                    break
                _, message_id, message = items
                task_name, task_args, task_kwargs = app._serializer.loads(message)
                await _execute_task(app, task_name, message_id, *task_args, **task_kwargs)

        logger.info(f'task loop is starting up')
        await asyncio.gather(*( inner_execute_loop() for _ in range(worker_task_concurrency) ))
        logger.info(f'task loop is stopped')
    
    async def loop():

        try:
            before_worker_startup = app._event_map.get('before_worker_startup')
            before_worker_startup and await before_worker_startup()
        except:
            logger.error('event handler "before_worker_startup" raised an exception', exc_info=True)

        await asyncio.gather(dequeue_loop(), execute_loop())

        pending_tasks = asyncio.all_tasks()
        pending_tasks.remove(asyncio.current_task())
        
        if pending_tasks:
            logger.warn(f'waiting for {len(pending_tasks)} unknown pending tasks, do not use "asyncio.shield" in tasks or event handlers')
            await asyncio.wait(pending_tasks, return_when=asyncio.ALL_COMPLETED)

        try:
            after_worker_shutdown = app._event_map.get('after_worker_shutdown')
            after_worker_shutdown and await after_worker_shutdown()
        except:
            logger.error('event handler "after_worker_shutdown" raised an exception', exc_info=True)
    
    def signal_handler(sig: Signals):
        if not loop_ctx['shutting_down']:
            loop_ctx['shutting_down'] = True
            logger.info(f'{str(sig)} received and worker is gracefully shutting down now')
    
    loop_ctx = dict(shutting_down = False)
    worker_queue = asyncio.PriorityQueue(maxsize = worker_queue_size)
    executor_pool = None
    executor_pool = ThreadPoolExecutor(executor_pool_size) if executor_pool_name == 'THREAD' else executor_pool
    executor_pool = ProcessPoolExecutor(executor_pool_size) if executor_pool_name == 'PROCESS' else executor_pool
    
    evloop = asyncio.get_event_loop()
    evloop.add_signal_handler(SIGINT, partial(signal_handler, SIGINT))
    evloop.add_signal_handler(SIGTERM, partial(signal_handler, SIGTERM))
    evloop.add_signal_handler(SIGHUP, partial(signal_handler, SIGHUP))

    cv_executor_pool.set(executor_pool)

    app._worker_started = True
    evloop.run_until_complete(loop())
    app._worker_started = False