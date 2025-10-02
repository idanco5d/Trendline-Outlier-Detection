import asyncio
from typing import Union

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from class_models import AlgoResponse, RawArgs, BackgroundAlgoResponse
from algorithm import run_algorithm, run_algorithm_background
from websockets import get_connection_manager
from stream import redis_stream_reader

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # TODO: add production frontend as well
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


@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    conn_manager = get_connection_manager()
    await conn_manager.connect(websocket, task_id)

    stream_task = asyncio.create_task(redis_stream_reader(task_id, websockets_manager=conn_manager))

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        stream_task.cancel()
        conn_manager.disconnect(websocket, task_id)