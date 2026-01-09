import time
import json
import random
import os
from datetime import datetime
from typing import List, Tuple
from pydantic import BaseModel, Field
from quixstreams import Application

# --- CONFIGURATION ---
TOPIC_NAME = "telemetry"
BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")

# --- DATA MODELS (Schema Validation) ---
# This ensures we never send garbage data to Kafka
class TelemetryEvent(BaseModel):
    truck_id: str
    timestamp: str
    latitude: float
    longitude: float
    temperature: float
    engine_rpm: int
    status: str
    route_id: str

# --- REAL-WORLD ROUTES (Waypoints) ---
# These are approximate coordinates along real Nigerian highways
ROUTES = {
    "TRUCK-101": [ # Lagos -> Ibadan -> Abuja (A1 HWY)
        (6.4550, 3.3941), # Lagos
        (6.5962, 3.3683), # Ikeja
        (6.8926, 3.7196), # Sagamu
        (7.3768, 3.9398), # Ibadan
        (7.7027, 4.4984), # Oshogbo
        (8.4904, 4.5522), # Ilorin
        (9.0563, 7.4985), # Abuja (Dest)
    ],
    "TRUCK-202": [ # Port Harcourt -> Enugu -> Makurdi
        (4.8156, 7.0498), # PH
        (5.4851, 7.0354), # Owerri
        (6.4584, 7.5464), # Enugu
        (7.7322, 8.5218), # Makurdi
        (8.8920, 9.5450), # Lafia
    ],
    "TRUCK-303": [ # Benin -> Okene -> Abuja
        (6.3392, 5.6175), # Benin City
        (7.1706, 6.1360), # Auchi
        (7.5629, 6.2343), # Okene
        (8.0069, 6.7455), # Lokoja
        (9.0600, 7.4800), # Abuja
    ]
}

class TruckSimulator:
    def __init__(self, truck_id: str, waypoints: List[Tuple[float, float]]):
        self.truck_id = truck_id
        self.waypoints = waypoints
        self.current_idx = 0
        self.progress = 0.0
        self.speed_factor = 0.02 # How fast we move between waypoints (1% per tick)
        self.status = "IN_TRANSIT"

    def move(self) -> Tuple[float, float]:
        """
        Moves the truck along the path defined by waypoints.
        Returns (current_lat, current_lon)
        """
        # Get current segment (Point A -> Point B)
        start_pt = self.waypoints[self.current_idx]
        end_pt = self.waypoints[(self.current_idx + 1) % len(self.waypoints)]

        # Interpolate
        lat = start_pt[0] + (end_pt[0] - start_pt[0]) * self.progress
        lon = start_pt[1] + (end_pt[1] - start_pt[1]) * self.progress

        # Advance progress
        self.progress += self.speed_factor
        
        # Check if we reached the next waypoint
        if self.progress >= 1.0:
            self.progress = 0.0
            self.current_idx = (self.current_idx + 1) % len(self.waypoints)
            
            # If we looped back to start, maybe pause or change status (Optional)
            if self.current_idx == 0:
                 self.status = "DOCKED"
            else:
                 self.status = "IN_TRANSIT"

        return lat, lon

    def generate_event(self) -> dict:
        current_lat, current_lon = self.move()

        # Simulate Temperature Physics
        # Normal: -20C. Failure Mode: If Truck 101 is near Lokoja/Abuja, fail.
        base_temp = -20.0
        
        # Inject Failure for TRUCK-101 midway through trip
        if self.truck_id == "TRUCK-101" and self.current_idx > 2 and self.current_idx < 5:
            base_temp = -2.0 # FAILURE: Freezer broke

        # Add Sensor Noise
        temp = base_temp + random.uniform(-0.5, 0.5)

        # Validate with Pydantic
        event = TelemetryEvent(
            truck_id=self.truck_id,
            timestamp=datetime.now().isoformat(),
            latitude=round(current_lat, 6),
            longitude=round(current_lon, 6),
            temperature=round(temp, 2),
            engine_rpm=random.randint(2000, 3000),
            status=self.status,
            route_id="RT-LAG-ABJ"
        )
        
        return event.model_dump()

def main():
    print(f"🚀 Starting Real-World Logistics Simulator...")
    print(f"📡 Connecting to Redpanda at {BROKER_ADDRESS}...")

    app = Application(broker_address=BROKER_ADDRESS)
    topic = app.topic(TOPIC_NAME, value_serializer="json")
    
    # Initialize Simulators
    simulators = [TruckSimulator(tid, pts) for tid, pts in ROUTES.items()]

    with app.get_producer() as producer:
        while True:
            for sim in simulators:
                event_data = sim.generate_event()

                # Manual Serialization for Robustness
                json_payload = json.dumps(event_data).encode('utf-8')
                key_payload = sim.truck_id.encode('utf-8')

                producer.produce(
                    topic=topic.name,
                    key=key_payload,
                    value=json_payload
                )
                
                print(f"🚛 {sim.truck_id} | {event_data['status']} | Temp: {event_data['temperature']}°C")

            time.sleep(0.5) # Update every 500ms for smoother animation

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Simulation Stopped.")