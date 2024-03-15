import os

QUEUE_INTERVAL = 2000
CRON_INTERVAL = 8000

DEFAULT_TASK_PRIORITY = 50
DEFAULT_TASK_RETRIES = 3
DEFAULT_TASK_TIMEOUT = 1000 * 60 * 10 
DEFAULT_TASK_QUEUE = 'default'
DEFAULT_TASK_QUEUE_SIZE = 2000

DEFAULT_CRON_RETRIES = 3
DEFAULT_CRON_TIMEOUT = 1000 * 60 * 10
DEFAULT_CRON_TIMEZONE = 'Asia/Shanghai'
DEFAULT_CRON_TIMEERROR = 1000 * 10
DEFAULT_CRON_EXPRESSION = '*/10 * * * *'

DEFAULT_WORKER_TASK_CONCURRENCY = 100
DEFAULT_WORKER_QUEUE_SIZE = 200

DEFAULT_EXECUTOR_POOL_SIZE = os.cpu_count() or 1
