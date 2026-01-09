import os
import json
from quixstreams import Application

# --- CONFIGURATION ---
BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")
INPUT_TOPIC = "telemetry"
OUTPUT_TOPIC = "alerts"

def main():
    print(f"🧠 Starting Logistics AI Processor...")
    print(f"📡 Connecting to {BROKER_ADDRESS}...")

    app = Application(
        broker_address=BROKER_ADDRESS,
        consumer_group="logistics-v2-processor",
        auto_offset_reset="latest" # Focus on real-time new data
    )

    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
    output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

    sdf = app.dataframe(input_topic)

    # --- RULE ENGINE ---
    
    # Rule 1: Temperature Excursion (> -5.0 C)
    # In production, we would use a sliding window here to avoid noise,
    # but for visualization, instant alerts feel snappier.
    sdf = sdf[sdf["temperature"] > -5.0]

    # Rule 2: Enrich with "SLA Violation" Metadata
    # This matches the AI's suggested payload structure
    sdf["alert_type"] = "TEMP_BREACH"
    sdf["alert_message"] = "Cargo Spoilage Risk Detected"
    sdf["severity"] = "CRITICAL"
    sdf["action_required"] = "INSPECT_IMMEDIATELY"

    # Print to console for debugging
    sdf.print()

    # Send to alerts topic
    sdf.to_topic(output_topic)

    app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Processor Stopped.")