"""
Logistics Watchtower - Production-Grade IoT Fleet Simulator
============================================================
Generates realistic telemetry data for a fleet of refrigerated trucks
with 10+ sensor types, discrete events, and physics-based simulation.
"""

import time
import json
import random
import os
import math
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Dict, Optional
from pydantic import BaseModel, Field
from quixstreams import Application

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

TOPIC_NAME = "telemetry"
BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "3"))
TICK_INTERVAL = float(os.getenv("TICK_INTERVAL", "0.5"))  # seconds between updates

# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS (Pydantic Schema Validation)
# ═══════════════════════════════════════════════════════════════════════════════

class DoorStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"

class EngineStatus(str, Enum):
    RUNNING = "RUNNING"
    OFF = "OFF"
    STARTING = "STARTING"

class TruckStatus(str, Enum):
    IN_TRANSIT = "IN_TRANSIT"
    LOADING = "LOADING"
    UNLOADING = "UNLOADING"
    DOCKED = "DOCKED"
    IDLE = "IDLE"
    MAINTENANCE = "MAINTENANCE"

class TelemetryEvent(BaseModel):
    """Full IoT telemetry payload with 15+ sensor readings"""
    # Identity
    truck_id: str
    timestamp: str
    route_id: str
    
    # GPS
    latitude: float
    longitude: float
    speed_kmh: float
    heading_degrees: float
    gps_accuracy_m: float
    
    # Refrigeration
    temperature_c: float
    humidity_pct: float
    compressor_status: str
    
    # Vehicle
    engine_status: str
    fuel_level_pct: float
    battery_voltage: float
    tire_pressure_psi: float
    odometer_km: float
    
    # Cargo
    door_status: str
    cargo_weight_kg: float
    
    # Operational
    status: str
    driver_id: str

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE DEFINITIONS (Real Nigerian Highways with Road Types)
# ═══════════════════════════════════════════════════════════════════════════════

# Each waypoint: (lat, lon, road_type, location_name)
ROUTES = {
    "RT-LAG-ABJ": {
        "name": "Lagos to Abuja Express",
        "waypoints": [
            (6.4550, 3.3941, "urban", "Lagos Mainland"),
            (6.5962, 3.3683, "urban", "Ikeja"),
            (6.8256, 3.6472, "highway", "Ikorodu"),
            (6.8926, 3.7196, "highway", "Sagamu Junction"),
            (7.3768, 3.9398, "urban", "Ibadan"),
            (7.7027, 4.4984, "highway", "Oshogbo"),
            (8.4904, 4.5522, "urban", "Ilorin"),
            (8.8500, 5.9667, "highway", "Mokwa"),
            (9.0563, 7.4985, "urban", "Abuja FCT"),
        ],
    },
    "RT-PHC-MKD": {
        "name": "Port Harcourt to Makurdi",
        "waypoints": [
            (4.8156, 7.0498, "urban", "Port Harcourt"),
            (5.1117, 7.3678, "highway", "Elele"),
            (5.4851, 7.0354, "urban", "Owerri"),
            (6.0072, 7.1194, "highway", "Okigwe"),
            (6.4584, 7.5464, "urban", "Enugu"),
            (6.8833, 7.3833, "highway", "Nsukka"),
            (7.7322, 8.5218, "urban", "Makurdi"),
        ],
    },
    "RT-BEN-ABJ": {
        "name": "Benin to Abuja",
        "waypoints": [
            (6.3392, 5.6175, "urban", "Benin City"),
            (6.7428, 6.0922, "highway", "Ekpoma"),
            (7.1706, 6.1360, "urban", "Auchi"),
            (7.5629, 6.2343, "highway", "Okene"),
            (8.0069, 6.7455, "urban", "Lokoja"),
            (8.5000, 7.1500, "highway", "Abaji"),
            (9.0600, 7.4800, "urban", "Abuja"),
        ],
    },
}

# Speed ranges by road type (km/h)
ROAD_SPEEDS = {
    "highway": (70, 100),
    "urban": (25, 45),
    "loading_zone": (0, 5),
}

# Driver pool
DRIVERS = ["DRV-001", "DRV-002", "DRV-003", "DRV-004", "DRV-005"]

# ═══════════════════════════════════════════════════════════════════════════════
# PHYSICS SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_heading(start: Tuple[float, float], end: Tuple[float, float]) -> float:
    """Calculate compass heading between two GPS points (degrees)"""
    lat1, lon1 = math.radians(start[0]), math.radians(start[1])
    lat2, lon2 = math.radians(end[0]), math.radians(end[1])
    
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    
    heading = math.degrees(math.atan2(x, y))
    return (heading + 360) % 360  # Normalize to 0-360

def haversine_distance(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    """Calculate distance between two GPS points in kilometers"""
    R = 6371  # Earth's radius in km
    lat1, lon1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lon2 = math.radians(p2[0]), math.radians(p2[1])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

# ═══════════════════════════════════════════════════════════════════════════════
# TRUCK SIMULATOR CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class TruckSimulator:
    """
    Simulates a single refrigerated truck with realistic sensor physics.
    Includes failure injection for testing alert systems.
    """
    
    def __init__(self, truck_id: str, route_id: str, driver_id: str):
        self.truck_id = truck_id
        self.route_id = route_id
        self.driver_id = driver_id
        
        # Get route data
        route_data = ROUTES.get(route_id, list(ROUTES.values())[0])
        self.waypoints = route_data["waypoints"]
        self.route_name = route_data["name"]
        
        # Position state
        self.current_waypoint_idx = 0
        self.segment_progress = 0.0  # 0.0 to 1.0 within current segment
        
        # Vehicle state
        self.status = TruckStatus.IN_TRANSIT
        self.engine_status = EngineStatus.RUNNING
        self.door_status = DoorStatus.CLOSED
        self.speed_kmh = 0.0
        self.heading = 0.0
        
        # Sensor values (with realistic initial states)
        self.temperature = random.uniform(-21.0, -19.0)  # Target: -20°C
        self.humidity = random.uniform(65.0, 75.0)  # Normal: 60-80%
        self.fuel_level = random.uniform(70.0, 100.0)  # Start with good fuel
        self.battery_voltage = random.uniform(13.5, 14.0)  # Healthy battery
        self.tire_pressure = random.uniform(105.0, 115.0)  # Normal: 100-120 PSI
        self.odometer = random.uniform(50000, 150000)  # Existing mileage
        self.cargo_weight = random.uniform(8000, 15000)  # kg
        
        # Failure injection flags
        self.compressor_failing = False
        self.door_malfunction = False
        self.low_fuel_mode = False
        
        # Timing
        self.ticks_at_stop = 0
        self.loading_ticks_remaining = 0
        
    def _get_current_road_type(self) -> str:
        """Get the road type for current segment"""
        return self.waypoints[self.current_waypoint_idx][2]
    
    def _get_current_location_name(self) -> str:
        """Get the location name for current waypoint"""
        return self.waypoints[self.current_waypoint_idx][3]
    
    def _inject_failures(self):
        """
        Inject realistic failure scenarios for testing.
        TRUCK-101 gets temperature failure mid-route.
        TRUCK-102 gets door malfunction.
        """
        # Temperature failure for first truck (waypoints 3-5)
        if self.truck_id.endswith("101"):
            if 3 <= self.current_waypoint_idx <= 5:
                self.compressor_failing = True
            else:
                self.compressor_failing = False
        
        # Door malfunction for second truck (random)
        if self.truck_id.endswith("102"):
            if self.current_waypoint_idx == 2 and self.segment_progress > 0.5:
                self.door_malfunction = True
            elif self.current_waypoint_idx > 3:
                self.door_malfunction = False
    
    def _update_position(self):
        """Move truck along route with realistic speed"""
        if self.status in [TruckStatus.LOADING, TruckStatus.UNLOADING, TruckStatus.DOCKED]:
            self.speed_kmh = 0.0
            return
        
        # Get current segment endpoints
        start_wp = self.waypoints[self.current_waypoint_idx]
        end_idx = (self.current_waypoint_idx + 1) % len(self.waypoints)
        end_wp = self.waypoints[end_idx]
        
        # Calculate speed based on road type
        road_type = self._get_current_road_type()
        min_speed, max_speed = ROAD_SPEEDS.get(road_type, (40, 60))
        
        # Add traffic variation
        traffic_factor = random.uniform(0.7, 1.0)
        target_speed = random.uniform(min_speed, max_speed) * traffic_factor
        
        # Smooth speed transition
        self.speed_kmh = self.speed_kmh * 0.8 + target_speed * 0.2
        
        # Calculate heading
        self.heading = calculate_heading(
            (start_wp[0], start_wp[1]),
            (end_wp[0], end_wp[1])
        )
        
        # Move along segment (progress based on distance and speed)
        segment_distance = haversine_distance(
            (start_wp[0], start_wp[1]),
            (end_wp[0], end_wp[1])
        )
        
        # Distance traveled this tick (km) = speed * time
        distance_this_tick = (self.speed_kmh / 3600) * TICK_INTERVAL
        progress_increment = distance_this_tick / max(segment_distance, 0.1)
        
        self.segment_progress += progress_increment
        self.odometer += distance_this_tick
        
        # Fuel consumption (roughly 0.3L/km for heavy truck)
        fuel_consumed = distance_this_tick * 0.3 / 400 * 100  # % of 400L tank
        self.fuel_level = max(0, self.fuel_level - fuel_consumed)
        
        # Check if reached next waypoint
        if self.segment_progress >= 1.0:
            self.segment_progress = 0.0
            self.current_waypoint_idx = end_idx
            
            # Check if completed route (loop back)
            if self.current_waypoint_idx == 0:
                self.status = TruckStatus.DOCKED
                self.ticks_at_stop = 0
            # Random chance to stop for loading at urban waypoints
            elif road_type == "urban" and random.random() < 0.15:
                self.status = TruckStatus.LOADING
                self.loading_ticks_remaining = random.randint(10, 30)
                self.door_status = DoorStatus.OPEN
    
    def _update_sensors(self):
        """Update all sensor values with physics simulation"""
        
        # === TEMPERATURE ===
        # Normal: compressor maintains -20°C
        # Failure: drifts toward ambient (+30°C)
        if self.compressor_failing:
            # Temperature rises toward ambient
            ambient = 30.0
            self.temperature += (ambient - self.temperature) * 0.02
        else:
            # Compressor cooling toward -20°C
            target = -20.0
            self.temperature += (target - self.temperature) * 0.1
        
        # Add sensor noise
        self.temperature += random.uniform(-0.3, 0.3)
        
        # === HUMIDITY ===
        # Rises if door is open
        if self.door_status == DoorStatus.OPEN:
            self.humidity = min(99.0, self.humidity + random.uniform(0.5, 2.0))
        else:
            # Slowly normalize
            self.humidity += (70.0 - self.humidity) * 0.05
        self.humidity += random.uniform(-1.0, 1.0)
        self.humidity = max(30.0, min(99.0, self.humidity))
        
        # === BATTERY ===
        if self.engine_status == EngineStatus.RUNNING:
            # Alternator charging
            self.battery_voltage = min(14.4, self.battery_voltage + random.uniform(0, 0.05))
        else:
            # Slow drain
            self.battery_voltage = max(11.0, self.battery_voltage - random.uniform(0, 0.02))
        self.battery_voltage += random.uniform(-0.1, 0.1)
        
        # === TIRE PRESSURE ===
        # Slow random variation
        self.tire_pressure += random.uniform(-0.5, 0.5)
        self.tire_pressure = max(70.0, min(130.0, self.tire_pressure))
        
    def _update_status(self):
        """Handle loading/unloading states"""
        if self.status == TruckStatus.LOADING:
            self.loading_ticks_remaining -= 1
            if self.loading_ticks_remaining <= 0:
                self.door_status = DoorStatus.CLOSED
                self.status = TruckStatus.IN_TRANSIT
                self.cargo_weight += random.uniform(500, 2000)
        
        elif self.status == TruckStatus.DOCKED:
            self.ticks_at_stop += 1
            if self.ticks_at_stop > 20:
                # Refuel and restart
                self.fuel_level = random.uniform(90, 100)
                self.status = TruckStatus.IN_TRANSIT
        
        # Door malfunction injection
        if self.door_malfunction and self.status == TruckStatus.IN_TRANSIT:
            self.door_status = DoorStatus.OPEN
        elif not self.door_malfunction and self.status == TruckStatus.IN_TRANSIT:
            self.door_status = DoorStatus.CLOSED
    
    def tick(self) -> dict:
        """
        Advance simulation by one tick and return telemetry event.
        """
        # Inject programmed failures
        self._inject_failures()
        
        # Update state
        self._update_status()
        self._update_position()
        self._update_sensors()
        
        # Get current position via interpolation
        start_wp = self.waypoints[self.current_waypoint_idx]
        end_idx = (self.current_waypoint_idx + 1) % len(self.waypoints)
        end_wp = self.waypoints[end_idx]
        
        current_lat = start_wp[0] + (end_wp[0] - start_wp[0]) * self.segment_progress
        current_lon = start_wp[1] + (end_wp[1] - start_wp[1]) * self.segment_progress
        
        # Add GPS noise
        gps_noise = random.uniform(0.00001, 0.00005)
        current_lat += random.choice([-1, 1]) * gps_noise
        current_lon += random.choice([-1, 1]) * gps_noise
        
        # Build telemetry event
        event = TelemetryEvent(
            truck_id=self.truck_id,
            timestamp=datetime.utcnow().isoformat() + "Z",
            route_id=self.route_id,
            
            latitude=round(current_lat, 6),
            longitude=round(current_lon, 6),
            speed_kmh=round(self.speed_kmh, 1),
            heading_degrees=round(self.heading, 1),
            gps_accuracy_m=round(random.uniform(2.0, 8.0), 1),
            
            temperature_c=round(self.temperature, 2),
            humidity_pct=round(self.humidity, 1),
            compressor_status="ON" if not self.compressor_failing else "FAULT",
            
            engine_status=self.engine_status.value,
            fuel_level_pct=round(self.fuel_level, 1),
            battery_voltage=round(self.battery_voltage, 2),
            tire_pressure_psi=round(self.tire_pressure, 1),
            odometer_km=round(self.odometer, 1),
            
            door_status=self.door_status.value,
            cargo_weight_kg=round(self.cargo_weight, 0),
            
            status=self.status.value,
            driver_id=self.driver_id,
        )
        
        return event.model_dump()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PRODUCER LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def create_fleet(size: int) -> List[TruckSimulator]:
    """Create a fleet of truck simulators"""
    fleet = []
    route_ids = list(ROUTES.keys())
    
    for i in range(size):
        truck_id = f"TRUCK-{101 + i}"
        route_id = route_ids[i % len(route_ids)]
        driver_id = DRIVERS[i % len(DRIVERS)]
        
        truck = TruckSimulator(truck_id, route_id, driver_id)
        fleet.append(truck)
        
    return fleet

def main():
    print("=" * 60)
    print("🚛 LOGISTICS WATCHTOWER - Production IoT Simulator")
    print("=" * 60)
    print(f"📡 Broker: {BROKER_ADDRESS}")
    print(f"🚚 Fleet Size: {FLEET_SIZE}")
    print(f"⏱️  Tick Interval: {TICK_INTERVAL}s")
    print("=" * 60)

    # Initialize Quix Streams
    app = Application(broker_address=BROKER_ADDRESS)
    topic = app.topic(TOPIC_NAME, value_serializer="json")
    
    # Create fleet
    fleet = create_fleet(FLEET_SIZE)
    print(f"✅ Initialized {len(fleet)} trucks:")
    for truck in fleet:
        print(f"   → {truck.truck_id} on {truck.route_name} (Driver: {truck.driver_id})")
    print("=" * 60)
    print("📤 Beginning telemetry stream...\n")

    # Main loop
    tick_count = 0
    with app.get_producer() as producer:
        while True:
            tick_count += 1
            
            for truck in fleet:
                # Generate telemetry
                event = truck.tick()
                
                # Serialize and send
                json_payload = json.dumps(event).encode('utf-8')
                key_payload = truck.truck_id.encode('utf-8')
                
                producer.produce(
                    topic=topic.name,
                    key=key_payload,
                    value=json_payload
                )
                
                # Console output (compact)
                temp_indicator = "🔴" if event["temperature_c"] > -5 else "🟢"
                door_indicator = "🚪" if event["door_status"] == "OPEN" else ""
                
                print(
                    f"{temp_indicator} {event['truck_id']:<10} | "
                    f"{event['status']:<12} | "
                    f"Temp: {event['temperature_c']:>6.1f}°C | "
                    f"Speed: {event['speed_kmh']:>5.1f} km/h | "
                    f"Fuel: {event['fuel_level_pct']:>5.1f}% {door_indicator}"
                )
            
            print(f"--- Tick {tick_count} | {datetime.now().strftime('%H:%M:%S')} ---")
            time.sleep(TICK_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped.")