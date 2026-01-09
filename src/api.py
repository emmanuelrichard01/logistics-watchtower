"""
Logistics Watchtower - Production API Gateway
==============================================
REST + WebSocket API with fleet state management,
metrics, and structured logging.
"""

import asyncio
import json
import logging
import os
import sys
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from confluent_kafka import Consumer

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")
TOPICS = ["telemetry", "alerts"]
API_VERSION = "2.0.0"
MAX_ALERT_HISTORY = 100

# ═══════════════════════════════════════════════════════════════════════════════
# STRUCTURED LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

class StructuredLogger:
    """JSON-formatted structured logging for production observability."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Console handler with JSON format
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
    
    def _log(self, level: str, event: str, **kwargs):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "service": self.name,
            "event": event,
            **kwargs
        }
        self.logger.info(json.dumps(log_entry))
    
    def info(self, event: str, **kwargs):
        self._log("INFO", event, **kwargs)
    
    def warning(self, event: str, **kwargs):
        self._log("WARNING", event, **kwargs)
    
    def error(self, event: str, **kwargs):
        self._log("ERROR", event, **kwargs)

logger = StructuredLogger("logistics-api")

# ═══════════════════════════════════════════════════════════════════════════════
# METRICS (Prometheus-style)
# ═══════════════════════════════════════════════════════════════════════════════

class Metrics:
    """Simple in-memory metrics for Prometheus scraping."""
    
    def __init__(self):
        self.messages_received = 0
        self.messages_by_topic: Dict[str, int] = {"telemetry": 0, "alerts": 0}
        self.alerts_by_type: Dict[str, int] = {}
        self.alerts_by_severity: Dict[str, int] = {}
        self.websocket_connections = 0
        self.websocket_messages_sent = 0
        self.errors = 0
        self.start_time = datetime.utcnow()
    
    def inc_messages(self, topic: str):
        self.messages_received += 1
        self.messages_by_topic[topic] = self.messages_by_topic.get(topic, 0) + 1
    
    def inc_alert(self, alert_type: str, severity: str):
        self.alerts_by_type[alert_type] = self.alerts_by_type.get(alert_type, 0) + 1
        self.alerts_by_severity[severity] = self.alerts_by_severity.get(severity, 0) + 1
    
    def to_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        
        lines = [
            "# HELP logistics_messages_total Total messages received",
            "# TYPE logistics_messages_total counter",
            f"logistics_messages_total {self.messages_received}",
            "",
            "# HELP logistics_messages_by_topic Messages by Kafka topic",
            "# TYPE logistics_messages_by_topic counter",
        ]
        
        for topic, count in self.messages_by_topic.items():
            lines.append(f'logistics_messages_by_topic{{topic="{topic}"}} {count}')
        
        lines.extend([
            "",
            "# HELP logistics_alerts_by_type Alerts by type",
            "# TYPE logistics_alerts_by_type counter",
        ])
        
        for alert_type, count in self.alerts_by_type.items():
            lines.append(f'logistics_alerts_by_type{{type="{alert_type}"}} {count}')
        
        lines.extend([
            "",
            "# HELP logistics_alerts_by_severity Alerts by severity",
            "# TYPE logistics_alerts_by_severity counter",
        ])
        
        for severity, count in self.alerts_by_severity.items():
            lines.append(f'logistics_alerts_by_severity{{severity="{severity}"}} {count}')
        
        lines.extend([
            "",
            "# HELP logistics_websocket_connections Current WebSocket connections",
            "# TYPE logistics_websocket_connections gauge",
            f"logistics_websocket_connections {self.websocket_connections}",
            "",
            "# HELP logistics_uptime_seconds Service uptime in seconds",
            "# TYPE logistics_uptime_seconds counter",
            f"logistics_uptime_seconds {uptime:.0f}",
            "",
            "# HELP logistics_errors_total Total errors",
            "# TYPE logistics_errors_total counter",
            f"logistics_errors_total {self.errors}",
        ])
        
        return "\n".join(lines)

metrics = Metrics()

# ═══════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

class FleetState:
    """In-memory state store for fleet tracking."""
    
    def __init__(self):
        self.trucks: Dict[str, dict] = {}
        self.alert_history: deque = deque(maxlen=MAX_ALERT_HISTORY)
        self.active_alerts: Dict[str, dict] = {}  # truck_id -> latest alert
    
    def update_truck(self, data: dict):
        """Update truck state from telemetry."""
        truck_id = data.get("truck_id")
        if not truck_id:
            return
        
        self.trucks[truck_id] = {
            "truck_id": truck_id,
            "last_seen": datetime.utcnow().isoformat() + "Z",
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "speed_kmh": data.get("speed_kmh", 0),
            "temperature_c": data.get("temperature_c", data.get("temperature")),
            "fuel_level_pct": data.get("fuel_level_pct"),
            "door_status": data.get("door_status"),
            "status": data.get("status"),
            "driver_id": data.get("driver_id"),
            "route_id": data.get("route_id"),
            "has_active_alert": truck_id in self.active_alerts,
        }
    
    def add_alert(self, alert: dict):
        """Record a new alert."""
        truck_id = alert.get("truck_id")
        
        # Add to history
        self.alert_history.appendleft(alert)
        
        # Track active alert per truck
        if truck_id:
            self.active_alerts[truck_id] = alert
        
        # Update metrics
        metrics.inc_alert(
            alert.get("alert_type", "UNKNOWN"),
            alert.get("severity", "UNKNOWN")
        )
    
    def get_fleet_summary(self) -> dict:
        """Get summary statistics for the fleet."""
        if not self.trucks:
            return {
                "total_trucks": 0,
                "trucks_with_alerts": 0,
                "trucks_in_transit": 0,
                "trucks_docked": 0,
            }
        
        in_transit = sum(1 for t in self.trucks.values() if t.get("status") == "IN_TRANSIT")
        docked = sum(1 for t in self.trucks.values() if t.get("status") == "DOCKED")
        with_alerts = len(self.active_alerts)
        
        return {
            "total_trucks": len(self.trucks),
            "trucks_with_alerts": with_alerts,
            "trucks_in_transit": in_transit,
            "trucks_docked": docked,
            "trucks_loading": sum(1 for t in self.trucks.values() if t.get("status") == "LOADING"),
        }
    
    def clear_stale_alerts(self, max_age_seconds: int = 60):
        """Clear alerts older than threshold (auto-resolve)."""
        now = datetime.utcnow()
        stale_trucks = []
        
        for truck_id, alert in self.active_alerts.items():
            try:
                alert_time = datetime.fromisoformat(alert.get("timestamp", "").replace("Z", ""))
                if (now - alert_time).total_seconds() > max_age_seconds:
                    stale_trucks.append(truck_id)
            except:
                pass
        
        for truck_id in stale_trucks:
            del self.active_alerts[truck_id]
            if truck_id in self.trucks:
                self.trucks[truck_id]["has_active_alert"] = False

fleet_state = FleetState()

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class ConnectionManager:
    """Manages WebSocket connections with tracking."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        metrics.websocket_connections = len(self.active_connections)
        logger.info("websocket_connected", connections=len(self.active_connections))
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        metrics.websocket_connections = len(self.active_connections)
        logger.info("websocket_disconnected", connections=len(self.active_connections))
    
    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients."""
        if not self.active_connections:
            return
        
        json_msg = json.dumps(message)
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(json_msg)
                metrics.websocket_messages_sent += 1
            except Exception as e:
                logger.error("websocket_send_error", error=str(e))
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()

# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA CONSUMER (Background Task)
# ═══════════════════════════════════════════════════════════════════════════════

def get_consumer():
    """Create Kafka consumer instance."""
    conf = {
        'bootstrap.servers': BROKER_ADDRESS,
        'group.id': 'logistics-api-v2',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    }
    return Consumer(conf)

async def consume_kafka_data():
    """Background task: Poll Kafka and broadcast to WebSockets."""
    logger.info("kafka_consumer_starting", broker=BROKER_ADDRESS, topics=TOPICS)
    
    consumer = get_consumer()
    consumer.subscribe(TOPICS)
    
    try:
        while True:
            msg = consumer.poll(0.1)
            
            if msg is None:
                await asyncio.sleep(0.01)
                # Periodically clear stale alerts
                fleet_state.clear_stale_alerts(max_age_seconds=30)
                continue
            
            if msg.error():
                logger.error("kafka_error", error=str(msg.error()))
                metrics.errors += 1
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                topic = msg.topic()
                
                # Update metrics
                metrics.inc_messages(topic)
                
                # Tag with topic for frontend
                data['topic'] = topic
                
                # Update state
                if topic == "telemetry":
                    fleet_state.update_truck(data)
                elif topic == "alerts":
                    fleet_state.add_alert(data)
                    fleet_state.update_truck(data)
                
                # Broadcast to WebSocket clients
                await manager.broadcast(data)
                
            except json.JSONDecodeError as e:
                logger.error("json_parse_error", error=str(e))
                metrics.errors += 1
            except Exception as e:
                logger.error("processing_error", error=str(e))
                metrics.errors += 1
    
    finally:
        consumer.close()
        logger.info("kafka_consumer_stopped")

# ═══════════════════════════════════════════════════════════════════════════════
# FASTAPI APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    logger.info("api_starting", version=API_VERSION)
    task = asyncio.create_task(consume_kafka_data())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("api_stopped")

app = FastAPI(
    title="Logistics Watchtower API",
    description="Real-time fleet monitoring and alerting API",
    version=API_VERSION,
    lifespan=lifespan,
)

# CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS (Response Schemas)
# ═══════════════════════════════════════════════════════════════════════════════

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    uptime_seconds: float
    kafka_broker: str

class FleetStatusResponse(BaseModel):
    summary: dict
    trucks: List[dict]
    timestamp: str

class TruckDetailResponse(BaseModel):
    truck: dict
    active_alert: Optional[dict]
    recent_alerts: List[dict]

class AlertHistoryResponse(BaseModel):
    alerts: List[dict]
    total: int
    timestamp: str

# ═══════════════════════════════════════════════════════════════════════════════
# REST ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/", response_model=HealthResponse, tags=["Health"])
def health_check():
    """Health check endpoint showing service status."""
    uptime = (datetime.utcnow() - metrics.start_time).total_seconds()
    return HealthResponse(
        status="online",
        service="Logistics Watchtower API",
        version=API_VERSION,
        uptime_seconds=round(uptime, 1),
        kafka_broker=BROKER_ADDRESS,
    )

@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health():
    """Alias for health check (Kubernetes readiness probe)."""
    return health_check()

@app.get("/fleet/status", response_model=FleetStatusResponse, tags=["Fleet"])
def get_fleet_status():
    """
    Get current status of all trucks in the fleet.
    Returns summary statistics and individual truck states.
    """
    return FleetStatusResponse(
        summary=fleet_state.get_fleet_summary(),
        trucks=list(fleet_state.trucks.values()),
        timestamp=datetime.utcnow().isoformat() + "Z",
    )

@app.get("/trucks/{truck_id}", response_model=TruckDetailResponse, tags=["Fleet"])
def get_truck_details(truck_id: str):
    """
    Get detailed information for a specific truck.
    Includes current state and recent alerts.
    """
    truck = fleet_state.trucks.get(truck_id)
    if not truck:
        raise HTTPException(status_code=404, detail=f"Truck {truck_id} not found")
    
    # Get alerts for this truck
    truck_alerts = [
        a for a in fleet_state.alert_history
        if a.get("truck_id") == truck_id
    ][:10]  # Last 10 alerts
    
    return TruckDetailResponse(
        truck=truck,
        active_alert=fleet_state.active_alerts.get(truck_id),
        recent_alerts=truck_alerts,
    )

@app.get("/alerts/recent", response_model=AlertHistoryResponse, tags=["Alerts"])
def get_recent_alerts(
    limit: int = Query(default=50, ge=1, le=100, description="Number of alerts to return"),
    severity: Optional[str] = Query(default=None, description="Filter by severity"),
    alert_type: Optional[str] = Query(default=None, description="Filter by alert type"),
):
    """
    Get recent alert history with optional filtering.
    """
    alerts = list(fleet_state.alert_history)
    
    # Apply filters
    if severity:
        alerts = [a for a in alerts if a.get("severity") == severity.upper()]
    if alert_type:
        alerts = [a for a in alerts if a.get("alert_type") == alert_type.upper()]
    
    # Apply limit
    alerts = alerts[:limit]
    
    return AlertHistoryResponse(
        alerts=alerts,
        total=len(alerts),
        timestamp=datetime.utcnow().isoformat() + "Z",
    )

@app.get("/alerts/active", tags=["Alerts"])
def get_active_alerts():
    """Get currently active alerts (not yet auto-resolved)."""
    return {
        "active_alerts": list(fleet_state.active_alerts.values()),
        "count": len(fleet_state.active_alerts),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/metrics", response_class=PlainTextResponse, tags=["Observability"])
def get_metrics():
    """
    Prometheus-format metrics endpoint.
    Scrape this endpoint for monitoring.
    """
    return metrics.to_prometheus()

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time telemetry streaming.
    Receives all telemetry and alert events.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive, accept any client messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("🔌 LOGISTICS WATCHTOWER - API Gateway")
    print("=" * 60)
    print(f"📡 Kafka Broker: {BROKER_ADDRESS}")
    print(f"📋 API Version: {API_VERSION}")
    print("=" * 60)
    print("📚 Endpoints:")
    print("   GET  /              - Health check")
    print("   GET  /fleet/status  - Fleet overview")
    print("   GET  /trucks/{id}   - Truck details")
    print("   GET  /alerts/recent - Alert history")
    print("   GET  /alerts/active - Active alerts")
    print("   GET  /metrics       - Prometheus metrics")
    print("   WS   /ws            - Real-time stream")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)