from asyncio import get_event_loop
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Any, NamedTuple, Callable, Union, Tuple
from contextvars import ContextVar

cv_task_info = ContextVar('task_info')
cv_task_groups = ContextVar('task_groups')
cv_task_result = ContextVar('task_result')
cv_task_exception = ContextVar('task_exception')
cv_task_retried = ContextVar('task_retried')
cv_executor_pool = ContextVar('executor_pool')

class TaskInfoNamedTuple(NamedTuple):
    task_id: bytes
    task_name: str
    task_queue: str
    task_priority: int
    task_retries: int
    task_timeout: int

def current_task_info() -> TaskInfoNamedTuple:
    return cv_task_info.get()

def current_task_groups() -> Tuple[str, ...]:
    return cv_task_groups.get()

def current_task_retried() -> int:
    return cv_task_retried.get()

def current_task_result() -> Any:
    return cv_task_result.get()

def current_task_exception() -> Exception:
    return cv_task_exception.get()

async def run_in_current_executor(fn: Callable) -> Any:
    executor : Union[None, ThreadPoolExecutor, ProcessPoolExecutor] = cv_executor_pool.get()
    return await get_event_loop().run_in_executor(executor, fn)