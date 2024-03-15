from typing import Tuple
from time import time_ns
from uuid import uuid4

from .app import Rediq
from .exceptions import (
    RedisQueueEmptyError, 
    RedisQueueFullError, 
    RedisQueueIntegrityError,
)

_QUEUE_KEY_PREFIX = 'rediq:queue_name:'

class Queue:

    _lua_enqueue = """
        local queue_key = KEYS[1]
        local queue_maxlen = tonumber(ARGV[1])
        local message_id = ARGV[2]
        local message_payload = ARGV[3]
        local score = tonumber(ARGV[4])

        if ( redis.call('ZCARD', queue_key) < queue_maxlen ) then
            return redis.call('ZADD', queue_key, 'NX', score, message_id, score, message_payload)
        end

        return -1
    """

    _lua_delete = """
        local queue_key = KEYS[1]
        local message_id = ARGV[1]

        local i = redis.call('ZRANK', queue_key, message_id)
        if ( i ~= false ) then
            return redis.call('ZREMRANGEBYRANK', queue_key, i, i + 1)
        end

        return 0
    """

    @staticmethod
    def _time_uuid() -> bytes:
        return time_ns().to_bytes(length=16, byteorder='big') + uuid4().bytes 

    def __init__(self, app: Rediq):

        self._app = app
        self._enqueue = self._app._redis.register_script(self._lua_enqueue)
        self._delete = self._app._redis.register_script(self._lua_delete)

    async def enqueue(
        self, 
        queue_name: str,
        queue_size: int,
        priority: int,
        message: bytes,
    ) -> bytes:

        message_id = self._time_uuid()
        message = message_id + message
        queue_key = _QUEUE_KEY_PREFIX + queue_name
        code = await self._enqueue(
            keys = [ queue_key ], 
            args = [ str(queue_size*2), message_id, message, str(priority) ],
        )
        if code < 0: 
            raise RedisQueueFullError('queue is full')
        if code != 2: 
            raise RedisQueueIntegrityError('queue data error')
        return message_id
    
    async def dequeue(
        self,
        queue_name: str,
        timeout: int,
    ) -> Tuple[int, bytes, bytes]:
        
        queue_key = _QUEUE_KEY_PREFIX + queue_name
        result = await self._app._redis.bzmpop(
            timeout = timeout / 1000,
            numkeys = 1,
            keys = [queue_key],
            min = True,
            count = 2,
        )
        if not result:
            raise RedisQueueEmptyError('queue is empty')
        if len(result[1]) != 2 or result[1][0][1] != result[1][1][1]:
            raise RedisQueueIntegrityError('queue data error')
        message_id, message_payload, message_priority = result[1][0][0], result[1][1][0], result[1][0][1]
        if type(message_id) is str:
            message_id = message_id.encode('utf-8')
        if type(message_payload) is str:
            message_payload = message_payload.encode('utf-8')
        message = message_payload[len(message_id):]
        message_priority = int(message_priority)
        return message_priority, message_id, message
    
    async def delete(
        self,
        queue_name: str,
        message_id: bytes,
    ) -> bool:
        
        queue_key = _QUEUE_KEY_PREFIX + queue_name
        code = await self._delete(
            keys = [ queue_key ],
            args = [ message_id ],
        )
        if code == 2: 
            return True
        if code == 0: 
            return False
        raise RedisQueueIntegrityError('queue data error')