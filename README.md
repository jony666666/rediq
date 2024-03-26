基于Redis的轻量、无状态、纯协程实现的Python分布式任务框架。

## Features
- Celery风格的任务注册发布接口(基于装饰器)
- Flask风格的任务信息获取接口(基于上下文)
- 所有队列操作都是原子的，并且无论成功失败都不产生中间数据
- 所有队列操作都是非轮询的，工作进程能立即从队列取回新入队的任务
- 同一个工作进程中的多个任务之间的取回阶段与执行阶段能完全并发的执行

## Installing
目前仅支持本地安装
```shell
$ pip install . --upgrade
$ rediq --help  # 安装完成后使用该命令启动worker
```

## Tutorial
1、创建Rediq实例并注册任务与事件处理器，注意被装饰的函数都必须是协程函数。
```python
# /tutorial/mytasks.py

import asyncio
import time
import functools

from rediq import (
    Rediq,
    current_task_exception,
    current_task_info,
    current_task_result,
    current_task_retried,
    run_in_current_executor,
)

app = Rediq(
    redis_url = 'redis://localhost:6379/0', # redis-py定义的URL格式
    redis_config = dict(
        decode_responses = False,
        max_connections = 10,
    ),  # redis-py的其他实例化的参数
    serializer_name = 'pickle', # 用于正反序列化任务的参数
)

# 注册任务，任务函数经过装饰器后会成为任务Publisher
@app.task(
    task_name='simple_task',  # 任务名称。不指定自动使用函数名，必须全局唯一
    task_priority=123,        # 任务优先级，越低越靠近队头，不指定时使用默认
    task_queue='a',           # 队列名称，不指定时使用默认
    task_retries=3,           # 任务异常时的重试次数，不指定时使用默认
    task_timeout=1000*10,     # 任务超时毫秒数，不指定时使用默认
)
async def simple_task(sec):

    await asyncio.sleep(sec)

    '''
        如果任务中存在计算/IO密集的同步函数，
        可以利用run_in_current_executor异步的执行，
        使用rediq启动worker时可以指定executor使用线程池/进程池。
    '''
    await run_in_current_executor(
        functools.partial(time.sleep, sec)
    )

    print(current_task_info())  # 获取当前任务的信息
    print(current_task_retried())  # 获取当前任务的重试轮数

    return 'simple_task finished'

# 注册事件，事件中的异常不会被抛出
@app.event(
    event_name="before_task_start" # 事件名称，不指定自动使用函数名
)
async def before_task_start():
    print(current_task_info()) # 获取任务id、注册任务时的各种参数

@app.event
async def before_task_retry():
    print(current_task_retried()) # 获取任务当前的重试次数
    
@app.event
async def after_task_success():
    print(current_task_result()) # 获取任务的返回值

@app.event
async def after_task_failure():
    print(current_task_exception()) # 获取任务的异常
    print(current_task_result())    # 在这里访问这个会抛异常

@app.event
async def after_worker_shutdown(): # worker停止后
    # await async_db.close()

@app.event
async def before_worker_startup(): # worker启动前
    # await async_db.connect()
```
2、利用rediq命令启动上述app实例的Worker进程，Worker在接收到SIGINT等信号后，会停止取回新任务，并等待正在执行的和已经预取的所有任务结束后才退出。
```shell
$ cd /tutorial

$ rediq \
    mytasks:app \
    --queue-name a --queue-name b \
    --skip-default-queue \
    --worker-name worker123 \
    --worker-queue-size 200 \
    --worker-task-concurrency 100 \
    --executor-pool-name process \
    --executor-pool-size 32 \
    --log-level info \
    --log-file 1.log

# rediq \
#    mytasks:app \                   使用该格式定位mytask.py中的app对象 
#    --queue-name a --queue-name b \ 指定需要消费的队列名称，可以消费多个队列
#    --skip-default-queue \          不消费默认队列(即注册任务时不指定task_queue的任务)
#    --worker-name worker123 \       指定该Worker的名称(最好全局唯一，以后会基于这一点加功能)
#    --worker-queue-size 200 \       指定Worker本地队列的大小，任务执行前会预取到该本地队列中
#    --worker-task-concurrency 100 \ 指定Worker中任务协程的最大并发数目
#    --executor-pool-name process \  指定run_in_current_executor使用进程池
#    --executor-pool-size 32 \       指定该进程池的最大进程数
#    --log-level info \              日志级别
#    --log-file 1.log                日志文件
```
3、通过app实例在其他业务进程中发布任务
```python
# /tutorial/api.py

from fastapi import FastAPI
from tasks import simple_task

app = FastAPI()

@app.get("/")
def _simple_task():
    task_id = await simple_task(123).enqueue() # 入队simple_task任务并返回任务ID
    return dict(task_id = task_id.hex())
```

4、函数 <code>current_xxx()</code> 都与当前的上下文有关，显然其中有的只能用于特定的事件处理函数中，具体对应关系如下：

| | run_in_current_executor | current_task_info | current_task_groups | current_task_retried | current_task_result | current_task_exception |
|-------|-------|-------|-------|-------|-------|-------|
| @task | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: |
| before_task_start | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: |:heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: |
| before_task_retry | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: | :heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: |
| after_task_success | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_multiplication_x: |
| after_task_failure | :heavy_check_mark: | :heavy_check_mark: |:heavy_check_mark: | :heavy_check_mark: | :heavy_multiplication_x: | :heavy_check_mark: |
| before_worker_startup | :heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: |
| after_worker_shutdown | :heavy_check_mark: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: | :heavy_multiplication_x: |

## TODO
- CRON模块
- 控制模块（根据ID从队列删除任务、取消执行中的任务、清空队列、查看队列信息、查看所有worker...）
- 日志模块改进
- 文档
- 注释
- 测试
- 英文
- Redis集群/哨兵...