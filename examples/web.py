import json
import time
import functools

from enum import IntEnum

from tortoise import fields, Tortoise
from tortoise.models import Model
from tortoise.contrib.pydantic import pydantic_model_creator

from fastapi import FastAPI

from rediq import (
    Rediq, 
    current_task_exception, 
    current_task_info, 
    current_task_result, 
    current_task_retried, 
    run_in_current_executor,
)

class TaskState(IntEnum):

    ENQUEUED = 1
    STARTED = 2
    RETRYING = 3
    SUCCESSFUL = 4
    FAILED = 5
    
class Task(Model):

    id = fields.UUIDField(pk=True)

    task_id = fields.CharField(max_length=64, unique=True, null=False)
    task_name = fields.CharField(max_length=32, unique=False, null=False)
    task_queue = fields.CharField(max_length=32, unique=False, null=False)
    task_priority = fields.IntField(unique=False, null=False)
    task_retries = fields.IntField(unique=False, null=False)
    task_timeout = fields.IntField(unique=False, null=False)

    task_state = fields.IntEnumField(TaskState, unique=False, null=False)
    task_retried = fields.IntField(unique=False, null=False)
    task_result = fields.TextField(unique=False, null=True)
    task_exception = fields.TextField(unique=False, null=True)

    created_at = fields.DatetimeField(auto_now_add=True)
    modified_at = fields.DatetimeField(auto_now=True)

PydanticTask = pydantic_model_creator(Task)

async def on_startup():
    await Tortoise.init(
        db_url = f'sqlite://tempdb.sqlite', 
        modules = dict(models = [ __name__ ]),
    )
    await Tortoise.generate_schemas()

async def on_shutdown():
    await Tortoise.close_connections()


rediq = Rediq(
    redis_url = 'redis://localhost:6379/0',
    serializer_name = 'pickle',
)

fastapi = FastAPI(
    debug = True
)

rediq.event(on_startup, event_name='before_worker_startup')
rediq.event(on_shutdown, event_name='after_worker_shutdown')

fastapi.add_event_handler('startup', on_startup)
fastapi.add_event_handler('shutdown', on_shutdown)

@rediq.event
async def after_task_enqueue():
    await Task(
        task_id = current_task_info().task_id.hex(),
        task_name = current_task_info().task_name,
        task_queue = current_task_info().task_queue,
        task_priority = current_task_info().task_priority,
        task_retries = current_task_info().task_retries,
        task_timeout = current_task_info().task_timeout,
        task_state = TaskState.ENQUEUED,
        task_retried = current_task_retried(),
        task_result = None,
        task_exception = None,
    ).save()

@rediq.event
async def before_task_start():
    await Task \
        .filter(task_id = current_task_info().task_id.hex()) \
        .update(task_state = TaskState.STARTED)

@rediq.event
async def before_task_retry():
    await Task \
        .filter(task_id = current_task_info().task_id.hex()) \
        .update(
            task_state = TaskState.RETRYING, 
            task_retry = current_task_retried(),
        )
    
@rediq.event
async def after_task_success():
    await Task \
        .filter(task_id = current_task_info().task_id.hex()) \
        .update(
            task_state = TaskState.SUCCESSFUL, 
            task_result = json.dumps(current_task_result())
        )

@rediq.event
async def after_task_failure():
    await Task \
        .filter(task_id = current_task_info().task_id.hex()) \
        .update(
            task_state = TaskState.FAILED, 
            task_exception = json.dumps(current_task_exception())
        )

@rediq.task
async def sleep_sec(sec: int) -> str:
    await run_in_current_executor(functools.partial(time.sleep, sec))
    return f'slept for {sec} seconds'

@fastapi.get('/get_task/{task_id}')
async def get_task(task_id: str) -> PydanticTask: # type: ignore
    return await Task.get(task_id = task_id)

@fastapi.get('/create_task/sleep_sec/{sec}')
async def task_sleep_sec(sec: int) -> dict:
    bytes_task_id = await sleep_sec(sec).enqueue()
    task_id = bytes_task_id.hex()
    return dict(task_id=task_id)


'''
$ cd examples
$ pip3 install fastapi==0.110.0 tortoise-orm==0.20.0 .. --upgrade
$ uvicorn web:fastapi --host 0.0.0.0 --reload
$ rediq web:rediq --executor-pool-name process

visit http://localhost:8000/docs#

'''