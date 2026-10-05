"""
Logistics Watchtower - Multi-Rule Stream Processor
===================================================
Advanced stream processing with 7+ alert rule types,
severity levels, and actionable recommendations.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any
from quixstreams import Application

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BROKER_ADDRESS = os.getenv("BROKER_ADDRESS", "localhost:9092")
INPUT_TOPIC = "telemetry"
OUTPUT_TOPIC = "alerts"
CONSUMER_GROUP = "logistics-processor-v3"

# ═══════════════════════════════════════════════════════════════════════════════
# ALERT RULE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

class AlertRules:
    """
    Centralized rule engine with thresholds and metadata.
    Each rule returns an alert dict or None.
    """
    
    @staticmethod
    def check_temperature_breach(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        CRITICAL: Temperature exceeds safe cold chain threshold.
        Cargo spoilage risk - requires immediate intervention.
        """
        temp = data.get("temperature_c", data.get("temperature", -20))
        
        if temp > -5.0:
            return {
                "alert_type": "TEMP_BREACH",
                "severity": "CRITICAL",
                "alert_message": f"Temperature excursion detected: {temp}°C exceeds -5°C threshold",
                "action_required": "DISPATCH_TECHNICIAN",
                "threshold": -5.0,
                "actual_value": temp,
                "unit": "°C",
                "sla_impact": "HIGH",
            }
        return None
    
    @staticmethod
    def check_humidity_breach(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        HIGH: Humidity indicates potential seal breach or door issue.
        """
        humidity = data.get("humidity_pct", 70)
        
        if humidity > 90.0:
            return {
                "alert_type": "HUMIDITY_BREACH",
                "severity": "HIGH",
                "alert_message": f"High humidity detected: {humidity}% indicates seal compromise",
                "action_required": "INSPECT_DOOR_SEALS",
                "threshold": 90.0,
                "actual_value": humidity,
                "unit": "%",
                "sla_impact": "MEDIUM",
            }
        return None
    
    @staticmethod
    def check_door_violation(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        CRITICAL: Door open while vehicle is moving.
        Safety hazard and cargo compromised.
        """
        door_status = data.get("door_status", "CLOSED")
        speed = data.get("speed_kmh", 0)
        
        if door_status == "OPEN" and speed > 5.0:
            return {
                "alert_type": "DOOR_VIOLATION",
                "severity": "CRITICAL",
                "alert_message": f"Door open while moving at {speed} km/h - immediate stop required",
                "action_required": "EMERGENCY_STOP",
                "threshold": 5.0,
                "actual_value": speed,
                "unit": "km/h",
                "sla_impact": "CRITICAL",
            }
        return None
    
    @staticmethod
    def check_speed_violation(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        MEDIUM: Excessive speed - safety and fuel efficiency concern.
        """
        speed = data.get("speed_kmh", 0)
        
        if speed > 100.0:
            return {
                "alert_type": "SPEED_VIOLATION",
                "severity": "MEDIUM",
                "alert_message": f"Speed limit exceeded: {speed} km/h (limit: 100 km/h)",
                "action_required": "NOTIFY_DRIVER",
                "threshold": 100.0,
                "actual_value": speed,
                "unit": "km/h",
                "sla_impact": "LOW",
            }
        return None
    
    @staticmethod
    def check_low_fuel(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        HIGH: Low fuel level - risk of stranding.
        """
        fuel = data.get("fuel_level_pct", 100)
        
        if fuel < 15.0:
            severity = "CRITICAL" if fuel < 5.0 else "HIGH"
            return {
                "alert_type": "LOW_FUEL",
                "severity": severity,
                "alert_message": f"Low fuel warning: {fuel}% remaining",
                "action_required": "ROUTE_TO_FUEL_STATION",
                "threshold": 15.0,
                "actual_value": fuel,
                "unit": "%",
                "sla_impact": "HIGH" if fuel < 5.0 else "MEDIUM",
            }
        return None
    
    @staticmethod
    def check_battery_low(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        MEDIUM: Low battery voltage - potential starting issues.
        """
        voltage = data.get("battery_voltage", 14.0)
        
        if voltage < 11.8:
            return {
                "alert_type": "BATTERY_LOW",
                "severity": "MEDIUM",
                "alert_message": f"Low battery voltage: {voltage}V (minimum: 11.8V)",
                "action_required": "SCHEDULE_MAINTENANCE",
                "threshold": 11.8,
                "actual_value": voltage,
                "unit": "V",
                "sla_impact": "LOW",
            }
        return None
    
    @staticmethod
    def check_tire_pressure(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        HIGH: Low tire pressure - safety and blowout risk.
        """
        pressure = data.get("tire_pressure_psi", 110)
        
        if pressure < 85.0:
            severity = "CRITICAL" if pressure < 70.0 else "HIGH"
            return {
                "alert_type": "TIRE_PRESSURE_LOW",
                "severity": severity,
                "alert_message": f"Low tire pressure: {pressure} PSI (minimum: 85 PSI)",
                "action_required": "STOP_AND_INSPECT",
                "threshold": 85.0,
                "actual_value": pressure,
                "unit": "PSI",
                "sla_impact": "HIGH",
            }
        return None
    
    @staticmethod
    def check_compressor_fault(data: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        CRITICAL: Compressor malfunction detected.
        """
        compressor = data.get("compressor_status", "ON")
        
        if compressor == "FAULT":
            return {
                "alert_type": "COMPRESSOR_FAULT",
                "severity": "CRITICAL",
                "alert_message": "Refrigeration compressor fault detected",
                "action_required": "DISPATCH_TECHNICIAN",
                "threshold": "ON",
                "actual_value": compressor,
                "unit": "status",
                "sla_impact": "CRITICAL",
            }
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# ALERT PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_all_rules(data: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Run all alert rules against telemetry data.
    Returns list of triggered alerts.
    """
    rules = [
        AlertRules.check_temperature_breach,
        AlertRules.check_humidity_breach,
        AlertRules.check_door_violation,
        AlertRules.check_speed_violation,
        AlertRules.check_low_fuel,
        AlertRules.check_battery_low,
        AlertRules.check_tire_pressure,
        AlertRules.check_compressor_fault,
    ]
    
    alerts = []
    for rule_fn in rules:
        result = rule_fn(data)
        if result:
            # Enrich alert with context from original telemetry
            alert = {
                **result,
                "truck_id": data.get("truck_id"),
                "timestamp": data.get("timestamp", datetime.utcnow().isoformat()),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "route_id": data.get("route_id", "UNKNOWN"),
                "driver_id": data.get("driver_id", "UNKNOWN"),
                "status": data.get("status", "UNKNOWN"),
            }
            alerts.append(alert)
    
    return alerts

def process_telemetry(data: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Process a single telemetry event.
    Returns the most severe alert, or None if no alerts triggered.
    """
    alerts = evaluate_all_rules(data)
    
    if not alerts:
        return None
    
    # Sort by severity priority
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    alerts.sort(key=lambda a: severity_order.get(a["severity"], 99))
    
    # Return most severe alert
    primary_alert = alerts[0]
    
    # Add count of total alerts for this event
    primary_alert["total_alerts"] = len(alerts)
    if len(alerts) > 1:
        primary_alert["additional_alerts"] = [a["alert_type"] for a in alerts[1:]]
    
    return primary_alert

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("🧠 LOGISTICS WATCHTOWER - Multi-Rule Stream Processor")
    print("=" * 60)
    print(f"📡 Broker: {BROKER_ADDRESS}")
    print(f"📥 Input Topic: {INPUT_TOPIC}")
    print(f"📤 Output Topic: {OUTPUT_TOPIC}")
    print(f"👥 Consumer Group: {CONSUMER_GROUP}")
    print("=" * 60)
    print("📋 Active Alert Rules:")
    print("   1. TEMP_BREACH     - Temperature > -5°C")
    print("   2. HUMIDITY_BREACH - Humidity > 90%")
    print("   3. DOOR_VIOLATION  - Door open while moving")
    print("   4. SPEED_VIOLATION - Speed > 100 km/h")
    print("   5. LOW_FUEL        - Fuel < 15%")
    print("   6. BATTERY_LOW     - Voltage < 11.8V")
    print("   7. TIRE_PRESSURE   - Pressure < 85 PSI")
    print("   8. COMPRESSOR_FAULT - Compressor status = FAULT")
    print("=" * 60)
    print("👂 Listening for telemetry...\n")

    # Initialize application
    app = Application(
        broker_address=BROKER_ADDRESS,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="latest"
    )

    # Define topics
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
    output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Apply multi-rule processing
    sdf = sdf.apply(process_telemetry)
    
    # Filter out None results (no alerts)
    sdf = sdf.filter(lambda x: x is not None)
    
    # Log alerts to console
    def log_alert(alert):
        severity_icons = {
            "CRITICAL": "🚨",
            "HIGH": "⚠️",
            "MEDIUM": "📢",
            "LOW": "ℹ️"
        }
        icon = severity_icons.get(alert["severity"], "❓")
        print(
            f"{icon} [{alert['severity']}] {alert['alert_type']:<18} | "
            f"{alert['truck_id']} | {alert['alert_message'][:50]}"
        )
        return alert
    
    sdf = sdf.apply(log_alert)
    
    # Send to alerts topic
    sdf.to_topic(output_topic)

    # Run the processor
    app.run(sdf)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Processor stopped.")