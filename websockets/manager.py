import threading
from typing import List, Dict
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connections: Dict[str, List[WebSocket]] = {}
        self._lock = threading.RLock()

    async def connect(self, websocket: WebSocket, task_id: str | None = None):
        with self._lock:
            await websocket.accept()
            self.active_connections.append(websocket)

            if task_id:
                if task_id not in self.connections:
                    self.connections[task_id] = []
                self.connections[task_id].append(websocket)

    def disconnect(self, websocket: WebSocket, task_id: str | None = None):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)

            if task_id and task_id in self.connections:
                if websocket in self.connections[task_id]:
                    self.connections[task_id].remove(websocket)
                if not self.connections[task_id]:
                    del self.connections[task_id]

    async def send_to_user(self, message: str, task_id: str):
        with self._lock:
            if task_id in self.connections:
                for connection in self.connections[task_id]:
                    try:
                        await connection.send_text(message)
                    except:
                        # Connection is probably closed, remove it
                        self.disconnect(connection, task_id)

_connection_manager = None
_connection_manager_lock = threading.Lock()

def get_connection_manager() -> ConnectionManager:
    global _connection_manager
    if _connection_manager is None:
        with _connection_manager_lock:
            if _connection_manager is None:
                _connection_manager = ConnectionManager()
    return _connection_manager