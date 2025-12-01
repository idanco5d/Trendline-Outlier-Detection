import os
import ssl

import redis
import redis.asyncio as async_redis

# --- Dependency Function for Redis Client ---
# Use a function to initialize and close the client
async def get_redis_pubsub_async_client() -> async_redis.Redis:
    # Use from_url to initialize the async client
    return async_redis.Redis.from_url(os.environ['REDIS_URL'], ssl_cert_reqs=ssl.CERT_NONE)

def get_redis_sync_client() -> redis.Redis:
    return redis.Redis.from_url(os.environ['REDIS_URL'], ssl_cert_reqs=ssl.CERT_NONE)

def redis_sync_client_cleanup(client: redis.Redis):
    if client and client.connection_pool:
        client.connection_pool.disconnect()

def get_pubsub_topic(task_id: str) -> str:
    return f'task:{task_id}'