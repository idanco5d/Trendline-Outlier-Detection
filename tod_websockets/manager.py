from fastapi import WebSocket

# Central manager to track which client is connected to which instance
class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        # Map: task_id (str) -> list of connected WebSocket clients
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, task_id: str, websocket: WebSocket):
        await websocket.accept()
        if task_id not in self.active_connections:
            self.active_connections[task_id] = []
        self.active_connections[task_id].append(websocket)

    def disconnect(self, task_id: str, websocket: WebSocket):
        try:
            self.active_connections[task_id].remove(websocket)
            if not self.active_connections[task_id]:
                del self.active_connections[task_id]
        except (KeyError, ValueError):
            pass # Already gone

    @classmethod
    async def send_message(cls, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except RuntimeError:
            # Handle client closed connection
            pass