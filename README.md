# 📡 Real-Time Data Ingestion & Streaming (Kafka Integration)

This repository serves as the **Data Ingestion & Streaming Layer** for the Gold Price Analytics Platform. It hosts high-frequency web scrapers and automated Apache Kafka Producers deployed as autonomous workloads on Kubernetes.

---

## 🏗️ Architectural Choice: Branch-per-Service (Microservices Isolation)

To prevent code coupling, guarantee strict deployment isolation, and support independent CI/CD pipelines for different financial data sources, this repository is architected using a **Branch-per-Service** strategy. 

Each financial data source operates as an independent microservice on its dedicated Git branch, preventing blast radiuses and allowing Kubernetes manifests (`deploy_producer.yaml`) to trigger updates independently without affecting other live ingestion pipelines.

---

## 📂 Data Sources Navigation

Click the links below to navigate to the source code and deployment manifests of each specific ingestion microservice:

| Data Source | Network/Protocol | Target Kafka Topic | Source Code Branch |
| :--- | :--- | :--- | :--- |
| **SJC (Domestic Gold)** | REST API / JSON | `sjc.raw` | [🔗 View Branch: `sjc`](https://github.com/Nu-xink-UIT/kafka-integration/tree/sjc) |
| **Goldprice (Global Gold)** | REST API / JSON | `goldprice.raw` | [🔗 View Branch: `goldprice`](https://github.com/Nu-xink-UIT/kafka-integration/tree/goldprice) |
| **Binance (Crypto Gold)** | WebSockets / Stream | `binance.raw` | [🔗 View Branch: `binance`](https://github.com/Nu-xink-UIT/kafka-integration/tree/binance) |
| **VCB (Foreign Exchange Rate)** | REST API / JSON | `vcb_exchange_rate` | [🔗 View Branch: `source4`](https://github.com/Nu-xink-UIT/kafka-integration/tree/vcb) |
| **PNJ (Alternative local source)** | REST API / JSON | `pnj.raw` | [🔗 View Branch: `source5`](https://github.com/Nu-xink-UIT/kafka-integration/tree/pnj) |

---

## ⚙️ Core Ingestor Blueprint 

Every individual branch follows a strict production-ready blueprint ensuring identical execution signatures on Kubernetes:

```text
.
├── README.md               # Specific source documentation
├── dockerfile              # Optimized multi-stage Docker build
├── deploy_producer.yaml    # GKE Deployment & Configuration manifests
├── requirements.txt        # Python isolation dependencies
├── schemas/                # JSON/Avro schema definitions for Kafka
└── src/
    └── [source_name]/
        ├── client/         # Specialized API/Scraper connection client
        ├── core/           # Shared Kafka Producer, Loggers & Config logic
        └── service/        # Polling/Streaming scheduling service
