import asyncio
import json
from typing import Union

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from class_models import AlgoResponse, RawArgs, BackgroundAlgoResponse
from algorithm import run_algorithm, run_algorithm_background
from pubsub import get_redis_pubsub_async_client, get_pubsub_topic
from tod_websockets import ConnectionManager

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],  # TODO: add production frontend as well
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.post('/', response_model=Union[AlgoResponse, BackgroundAlgoResponse])
def algorithm(body: RawArgs, background: bool = False):
    if not background:
        _, subset_df, removed_df = run_algorithm(initial_args=body)
        return AlgoResponse(subset_df=subset_df.to_dict('records'),
                            removed_df=removed_df.to_dict('records'))
    else:
        task = run_algorithm_background.delay(body.model_dump())
        return BackgroundAlgoResponse(task_id=task.id)

manager = ConnectionManager()


# --- Websocket Endpoint ---
@app.websocket("/ws/task/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await manager.connect(task_id, websocket)
    topic = get_pubsub_topic(task_id)

    # 2. Get the async Redis client and PubSub connection
    redis_client = await get_redis_pubsub_async_client()
    pubsub = redis_client.pubsub()

    # 3. Subscribe to the specific task topic
    await pubsub.subscribe(topic)

    # This task listens to Redis and relays messages to the client
    async def pubsub_listener():
        while True:
            # Use get_message to safely poll the pubsub connection in an async loop
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            if message and message['type'] == 'message':
                # The message['data'] is bytes, decode it to a string
                data_str = message['data'].decode('utf-8')

                # Send the message to the connected WebSocket client
                await manager.send_message(data_str, websocket)

                as_json = json.loads(data_str)
                # Stop listening (task is done)
                if as_json.get('done') is True:
                    break

            # This is a good place to yield control back to the event loop
            await asyncio.sleep(0.01)

    # 4. Run the listener concurrently with the main websocket loop
    listener_task = asyncio.create_task(pubsub_listener())

    try:
        # Keep the connection open and wait for client disconnect
        while True:
            # You can handle incoming client messages here if needed,
            # but for a server-sent status update, we just keep the loop alive.
            await websocket.receive_text()

    except WebSocketDisconnect:
        print(f"Client disconnected from task {task_id}.")
    finally:
        # 5. Crucial Cleanup: Stop the listener and close the Redis connection
        listener_task.cancel()
        await pubsub.unsubscribe(topic)
        await redis_client.close()
        manager.disconnect(task_id, websocket)