import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from confluent_kafka import Consumer
from typing import List

# FIX: Read from Environment Variable
BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")
TOPICS = ["telemetry", "alerts"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

def get_consumer():
    conf = {
        'bootstrap.servers': BROKER_ADDRESS,
        'group.id': 'dashboard-api',
        'auto.offset.reset': 'latest'
    }
    return Consumer(conf)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        json_msg = json.dumps(message)
        for connection in self.active_connections:
            try:
                await connection.send_text(json_msg)
            except Exception as e:
                logger.error(f"Error sending to socket: {e}")

manager = ConnectionManager()

async def consume_kafka_data():
    logger.info(f"📡 Connecting Consumer to {BROKER_ADDRESS}...")
    consumer = get_consumer()
    consumer.subscribe(TOPICS)

    try:
        while True:
            msg = consumer.poll(0.1) 
            if msg is None:
                await asyncio.sleep(0.01)
                continue
            if msg.error():
                logger.error(f"Kafka Error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                data['topic'] = msg.topic()
                await manager.broadcast(data)
            except Exception as e:
                logger.error(f"Parse Error: {e}")
    finally:
        consumer.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_kafka_data())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.get("/")
def health():
    return {"status": "online", "service": "Logistics Socket API"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)