import asyncio
import json
import logging
import os
import redis
import redis.asyncio as async_redis

from functools import wraps

from websockets import ConnectionManager

def _get_stream_key(task_id: str) -> str:
    return f"stream:task:{task_id}"


def with_redis_stream(func):
    @wraps(func)
    def wrapper(task_id: str, *args, **kwargs):
        with redis.from_url(
                url=os.environ['REDIS_URL'],
                decode_responses=True
        ) as redis_client:
            result = func(*args, **kwargs)

            message = {
                "task_id": task_id,
                "result": result
            }

            stream_key = _get_stream_key(task_id)
            redis_client.xadd(stream_key, message)
    return wrapper


async def redis_stream_reader(task_id: str, websockets_manager: ConnectionManager):
    stream_key = _get_stream_key(task_id)
    last_id = '0-0'

    async with async_redis.from_url(url=os.environ['REDIS_URL'], decode_responses=True) as redis_conn:
        while True:
            try:
                messages = await redis_conn.xread({stream_key: last_id}, block=1000, count=10)

                for stream, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        last_id = msg_id
                        await websockets_manager.send_to_user(json.dumps(msg_data), task_id)
            except Exception as e:
                logging.error(f"Stream reader error: {e}")
                await asyncio.sleep(1)