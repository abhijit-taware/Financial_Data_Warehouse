# 📊 Financial Trading Data Warehouse

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Setup Instructions](#setup-instructions)
- [Service Details](#service-details)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Directory & Permissions](#directory--permissions)
- [Troubleshooting](#troubleshooting)
- [Useful Files](#useful-files)
- [Contributing](#contributing)
- [Contact](#contact)

---

## Overview

The **Financial Trading Data Warehouse** is a modular, containerized data platform for ingesting, transforming, and storing both real-time and daily financial market data (stocks and cryptocurrencies). It is designed for scalability, automation, and ease of deployment using Docker Compose.

---

## Architecture

```
[API Providers] 
     | 
     v
[Airflow DAGs] --> [Kafka] --> [Spark (PySpark)] --> [PostgreSQL/Delta Lake]
     |                                   ^
     |                                   |
     +-------> [Jupyter Notebooks] <------+
```

- **Airflow** orchestrates the workflow.
- **Kafka** streams raw data.
- **Spark** transforms and processes data.
- **PostgreSQL/Delta Lake** stores processed data.
- **Jupyter** enables interactive analysis.

---

## Features

- **Automated Data Ingestion:** Scheduled and on-demand extraction of OHLCV data for stocks and cryptocurrencies.
- **Real-Time Streaming:** Kafka-based streaming for low-latency data flows.
- **Batch Processing:** PySpark jobs for scalable data transformation.
- **Workflow Orchestration:** Airflow DAGs manage dependencies and scheduling.
- **Interactive Analysis:** Jupyter notebooks for data exploration and prototyping.
- **Containerized Deployment:** All services run in isolated Docker containers for reproducibility.

---

## Project Structure

```
Financial_Trading_Data_Warehouse/
├── .env
├── docker-compose.yml
├── How_to_run_pipeline.txt
├── manual_directory_creation.txt
├── airflow/
│   ├── Dockerfile
│   ├── airflow.cfg
│   ├── requirements.txt
│   ├── webserver_config.py
│   ├── dags/
│   │   └── daily_market_data_pipeline/
│   │       ├── daily_market_data_pipeline.py
│   │       └── tasks/
│   │           ├── check_market_hours.py
│   │           ├── determine_extraction.py
│   │           ├── extract_crypto_data.py
│   │           ├── extract_stocks_data.py
│   │           ├── stream_data_using_kafka.py
│   │           ├── transform_crypto_data_spark.py
│   │           ├── transform_stock_data_spark.py
│   │           ├── validate_api_connections.py
│   │           ├── create_spark_default_conn.py
│   │           └── path_and_permession_setup.py
├── jupyter/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── notebooks/
├── kafka/
│   └── data/
├── postgres/
│   └── data/
├── redis/
│   └── data/
├── spark/
│   └── scripts/
├── sqlserver/
│   └── data/
├── zookeeper/
│   └── data/
└── README.md
```

---

## Tech Stack

- **Apache Airflow**: Workflow orchestration and scheduling
- **Apache Kafka**: Real-time data streaming
- **Apache Spark (PySpark)**: Distributed data processing
- **PostgreSQL / Delta Lake**: Data storage
- **Jupyter Notebook**: Interactive data analysis
- **Docker Compose**: Container orchestration

---

## Setup Instructions

### 1. Clone the Repository

```sh
git clone https://github.com/your-username/your-repo.git
cd Financial_Trading_Data_Warehouse
```

### 2. Configure Environment Variables

Edit the `.env` file to set your API keys, database credentials, and port mappings.

### 3. Create Required Directories

Some directories must be created manually for data persistence and permissions.

**On Windows (PowerShell):**
```sh
mkdir airflow\logs
mkdir airflow\dags
mkdir airflow\plugins
mkdir airflow\data\processed\crypto
mkdir airflow\data\checkpoints\crypto
mkdir airflow\data\processed\stock
mkdir airflow\data\checkpoints\stock
icacls "airflow\data" /grant Everyone:F /T
```

**On Linux/macOS:**
```sh
mkdir -p airflow/logs airflow/dags airflow/plugins
mkdir -p airflow/data/processed/crypto airflow/data/checkpoints/crypto
mkdir -p airflow/data/processed/stock airflow/data/checkpoints/stock
chmod -R 777 airflow/data
```

### 4. Build and Start Services

```sh
docker-compose build airflow-init
docker-compose up --build -d
```

### 5. Access Services

- **Airflow UI:** [http://localhost:8081](http://localhost:8081)
- **Jupyter Notebook:** [http://localhost:8888](http://localhost:8888)
- **Spark UI:** [http://localhost:4040](http://localhost:4040) (when a Spark job is running)
- **Kafka UI (if configured):** [http://localhost:9021](http://localhost:9021)

### 6. Shutdown

```sh
docker-compose down
```

---

## Service Details

### Airflow

- **Purpose:** Orchestrates ETL workflows via DAGs.
- **Location:** `airflow/`
- **Custom DAGs:** Located in `airflow/dags/daily_market_data_pipeline/`
- **Web UI:** [http://localhost:8081](http://localhost:8081)

### Kafka

- **Purpose:** Streams raw market data for real-time processing.
- **Topics:** Configurable in `.env` and DAGs.

### Spark

- **Purpose:** Transforms and cleanses data at scale.
- **Scripts:** Located in `spark/scripts/`
- **UI:** [http://localhost:4040](http://localhost:4040) (active during jobs)

### Jupyter

- **Purpose:** Interactive data analysis and prototyping.
- **Notebooks:** Place in `jupyter/notebooks/`

### PostgreSQL / Delta Lake

- **Purpose:** Persistent storage for processed data.
- **Configuration:** Set credentials in `.env`

---

## Data Pipeline Workflow

**DAG:** `daily_market_data_pipeline`

| Step                        | Description                                    |
|-----------------------------|------------------------------------------------|
| `check_market_hours`        | Checks if the market is open                   |
| `validate_api_connections`  | Ensures API endpoints are reachable            |
| `extract_stocks_data`       | Fetches OHLCV data for stocks                  |
| `extract_crypto_data`       | Fetches OHLCV data for cryptocurrencies        |
| `stream_data_using_kafka`   | Publishes raw data to Kafka topics             |
| `transform_stock_data_spark`| Cleans and transforms stock data with Spark    |
| `transform_crypto_data_spark`| Cleans and transforms crypto data with Spark  |
| `load_to_storage`           | Loads processed data into PostgreSQL/Delta Lake|

---

## Directory & Permissions

- **Kafka/Zookeeper:** Clear `kafka/data` and `zookeeper/data` before each run to avoid state issues.
- **Airflow Data:** Ensure `airflow/data` and subfolders have full permissions.
- **Windows Users:** Use `icacls` for permissions as shown above.

---

## Troubleshooting

- **Spark UI not accessible:**  
  Ensure port `4040` is mapped in `docker-compose.yml` and only one Spark job is running (otherwise, check ports 4041, 4042, etc.).

- **Kafka/Zookeeper errors:**  
  Clear `kafka/data` and `zookeeper/data` directories before restarting services.

- **Permission errors:**  
  Double-check directory permissions, especially on Windows.

- **Airflow DAGs not showing:**  
  Ensure DAG files are in the correct `airflow/dags/` subdirectory and Airflow has access.

---

## Useful Files

- `docker-compose.yml`: Service orchestration
- `.env`: Environment variables and secrets
- `How_to_run_pipeline.txt`: Step-by-step pipeline instructions
- `manual_directory_creation.txt`: Manual directory setup (Windows)

---

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature`)
3. Commit your changes
4. Push to your fork and submit a pull request

---

## Contact

For questions, suggestions, or contributions, please open an issue or pull request on the
