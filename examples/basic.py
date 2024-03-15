import asyncio
import threading, multiprocessing

from rediq import (
    Rediq,
    current_task_exception,
    current_task_info,
    current_task_result,
    current_task_retried,
    run_in_current_executor,
)

rediq = Rediq(
    redis_url = 'redis://localhost:6379/0',
    serializer_name = 'pickle',
)

def executor_inner():
    print(
        'this is executed inside executor',
        threading.current_thread(),
        multiprocessing.current_process(),
    )

def return_exception(fn):
    def inner(*a, **b):
        try:
            return fn(*a, **b)
        except Exception as e:
            return e
    return inner

@rediq.task
async def test1():

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print(e)

    print(
        'test1', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    raise RuntimeError('test1 triggered retrying')

@rediq.task
async def test2():

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print(e)

    print(
        'test2', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    return 'test2 finished'

@rediq.event
async def after_task_enqueue():

    print(
        'after_task_enqueue', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print('after_task_enqueue', e)

@rediq.event
async def before_task_start():

    print(
        'before_task_start', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print('before_task_start', e)

@rediq.event
async def before_task_retry():

    print(
        'before_task_retry', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print('before_task_retry', e)

@rediq.event
async def after_task_success():

    print(
        'after_task_success', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print('after_task_success', e)

@rediq.event
async def after_task_failure():

    print(
        'after_task_failure', 
        return_exception(current_task_info)(),
        return_exception(current_task_retried)(),
        return_exception(current_task_result)(),
        return_exception(current_task_exception)(),
    )

    try:
        await run_in_current_executor(executor_inner)
    except Exception as e:
        print('after_task_failure', e)

if __name__ == '__main__':
    
    async def main():
        await test1().enqueue()
        await test2().enqueue()

    evloop = asyncio.get_event_loop()
    evloop.run_until_complete(main())

'''
$ cd examples
$ pip3 install .. --upgrade
$ rediq basic:rediq --executor-pool-name process
$ python3 basic.py

'''