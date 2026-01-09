# **🚛 Real-Time Logistics Watchtower (Cold Chain Intelligence)**

**An event-driven streaming platform designed to detect temperature excursions in cold-chain logistics with sub-second latency.**

## **1️⃣ Executive Summary**

Cold chain logistics fail when sensitive cargo (pharmaceuticals, perishables) exceeds safe temperature thresholds. Traditional monitoring often relies on "post-trip" data dumps, identifying spoilage only after delivery.

**Logistics Watchtower** enables **active intervention** by processing IoT telemetry in-flight. It ingests high-velocity sensor data, applies stateful stream processing to filter noise, and pushes critical alerts to a live command center instantly.

**System Capabilities:**

* **Latency:** End-to-end processing (Sensor $\\to$ Dashboard) in \< 200ms.  
* **Throughput:** Scalable ingestion using **Redpanda** (C++ Kafka).  
* **Reliability:** Schema enforcement via Pydantic and containerized microservices architecture.

## **2️⃣ System Architecture**

The platform implements a decoupled **Event-Driven Architecture (EDA)** where components communicate exclusively via message passing.

graph LR  
    subgraph IoT\_Edge \["IoT Edge (Simulation)"\]  
        Trucks\[🚚 Smart Producer\]  
    end

    subgraph Infrastructure \["Streaming Infrastructure"\]  
        Redpanda\[🔴 Redpanda Broker\]  
    end

    subgraph Processing \["Processing Layer"\]  
        StreamProc\[⚙️ Quix Stream Processor\]  
    end

    subgraph Serving \["Serving Layer"\]  
        API\[🔌 FastAPI WebSocket Gateway\]  
    end

    subgraph UI \["User Interface"\]  
        Map\[🗺️ Live Map Dashboard\]  
    end

    Trucks \-- "JSON Telemetry" \--\> Redpanda  
    Redpanda \-- "Topic: telemetry" \--\> StreamProc  
      
    StreamProc \-- "Filter & Enrich" \--\> StreamProc  
    StreamProc \-- "Topic: alerts" \--\> Redpanda  
      
    Redpanda \-- "Consume (Both Topics)" \--\> API  
    API \-- "WebSocket Push" \--\> Map

### **Component Responsibilities**

| Component | Tech Stack | Responsibility |
| :---- | :---- | :---- |
| **Broker** | **Redpanda** | The central log for all events. Acts as the decoupling buffer between producers and consumers. |
| **Stream Processor** | **Quix Streams** | Stateless filtration and transformation. Consumes raw telemetry, applies threshold logic (Temp \> \-5°C), and produces standardized alerts. |
| **Gateway** | **FastAPI** | Bridges the streaming world (Kafka protocol) and the web world (WebSockets). Manages client connections and broadcasts events. |
| **Producer** | **Python/Pydantic** | Generates synthetic but realistic high-frequency sensor data with schema validation. |

## **3️⃣ Engineering Decisions & Trade-offs**

### **1\. Broker: Redpanda vs. Apache Kafka**

* **Decision:** Selected **Redpanda**.  
* **Trade-off:** Redpanda is a newer entrant compared to the battle-tested JVM-based Kafka.  
* **Justification:** For containerized microservice environments, Redpanda's **C++ architecture** (Thread-per-core model) eliminates the JVM overhead and ZooKeeper dependency. This results in significantly faster startup times and lower memory footprint for local development, while maintaining full Kafka API compatibility.

### **2\. Processing: Quix Streams vs. Apache Spark/Flink**

* **Decision:** Selected **Quix Streams** (Python).  
* **Trade-off:** Spark/Flink offer massive horizontal scaling for petabyte-scale data but introduce immense operational complexity (Cluster management, JAR submissions).  
* **Justification:** The logic required (filtering and thresholding) is compute-light but latency-sensitive. Quix provides a **Python-native** interface for stream processing without the context switch to Java/Scala, allowing for rapid iteration and easier maintainability for this specific use case.

### **3\. Transport: WebSockets vs. REST Polling**

* **Decision:** Selected **WebSockets** (ws://).  
* **Trade-off:** WebSockets require managing persistent connections and state on the server, whereas REST is stateless and easier to scale behind load balancers.  
* **Justification:** A "Live Map" requires fluidity. REST polling (e.g., every 1s) introduces artificial latency and unnecessary network overhead (headers/handshakes). WebSockets enable an **Event-Push** model, ensuring the dashboard reflects the state of the system the millisecond an event is processed.

### **4\. Simulation: Physics-Based vs. Random Generation**

* **Decision:** Implemented **Linear Interpolation (Lerp)** along real highway coordinates.  
* **Justification:** Random coordinate generation creates chaotic, unrealistic "teleportation" on maps, making it impossible to validate geospatial features. By interpolating between specific waypoints (e.g., Lagos $\\to$ Ibadan), the system produces realistic vector movement, allowing for future features like "Route Deviation Detection."

## **4️⃣ Quick Start (Local Production)**

The entire platform is containerized via Docker Compose.

### **Prerequisites**

* Docker Engine & Docker Compose

### **Launch Instructions**

\# 1\. Clone the repository  
git clone \[https://github.com/emmanuelrichard01/logistics-watchtower.git\](https://github.com/emmanuelrichard01/logistics-watchtower.git)  
cd logistics-watchtower

\# 2\. Start the microservices fleet  
docker-compose up \--build

### **Access Points**

* **Live Dashboard:** http://localhost:8000/src/frontend/index.html (or open file directly)  
* **Redpanda Console:** http://localhost:8080 (For debugging topics/offsets)

### **Verification**

1. Open the Dashboard. You should see markers moving along Nigerian highway routes.  
2. Watch for **TRUCK-101**. It is programmed to simulate a cooling failure (Temp $\\approx$ \-2°C) mid-route.  
3. Observe the marker turn **RED** and trigger an alert popup instantly.

## **5️⃣ Directory Structure**

logistics-watchtower/  
├── docker-compose.yaml     \# Orchestration of Broker, API, and Workers  
├── src/  
│   ├── producer.py         \# Physics-based simulator with Pydantic validation  
│   ├── processor.py        \# Stream processing logic (Filter/Enrich)  
│   ├── api.py              \# Async WebSocket gateway  
│   └── frontend/           \# Leaflet.js visualization  
└── requirements.txt        \# Dependencies (Quix, FastAPI, Confluent-Kafka)

## **👤 Author**

Emmanuel Richard  
Data Engineer focused on Resilient Streaming Systems