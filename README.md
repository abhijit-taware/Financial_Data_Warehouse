# 📊 Financial Data Warehouse

## 📌 Project Overview

**Financial Data Warehouse** is a modular and automated data pipeline designed for ingesting, transforming, and storing real-time and daily financial market data (stocks and cryptocurrencies). It leverages **Apache Airflow**, **Kafka**, **PySpark**, and **Docker** to build a scalable, production-ready data platform.

---

## 🚀 Features

- Daily and real-time data ingestion of stock and crypto OHLCV data.
- Data transformation using Apache Spark for large-scale processing.
- Real-time data streaming with Kafka.
- Workflow orchestration using Apache Airflow.
- Fully containerised with Docker Compose for easy deployment.

---

## 🗂️ Project Structure

Financial_Data_Warehouse/
├── .env
├── docker-compose.yml
├── How_to_run_pipeline.txt
├── manual_directory_creation.txt
├── airflow/
│ ├── Dockerfile
│ ├── airflow.cfg
│ ├── requirements.txt
│ ├── webserver_config.py
│ ├── dags/
│ │ └── daily_market_data_pipeline/
│ │ ├── daily_market_data_pipeline.py
│ │ └── tasks/
│ │ ├── check_market_hours.py
│ │ ├── determine_extraction.py
│ │ ├── extract_crypto_data.py
│ │ ├── extract_stocks_data.py
│ │ ├── stream_data_using_kafka.py
│ │ ├── transform_crypto_data_spark.py
│ │ ├── transform_stock_data_spark.py
│ │ ├── validate_api_connections.py
│ │ ├── create_spark_default_conn.py
│ │ └── path_and_permession_setup.py
└── README.md


---

## ⚙️ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo

mkdir -p airflow/{logs, dags, plugins, data}
mkdir jupyter

docker-compose up --build


---

### ✅ Part 3: DAG & Tech Stack

```markdown
---

## 📌 DAG Workflow Breakdown

**DAG Name**: `daily_market_data_pipeline`

| Task                        | Description                                  |
|----------------------------|----------------------------------------------|
| `check_market_hours`       | Ensures markets are open before extraction   |
| `validate_api_connections` | Validates health of APIs (e.g., Alpha Vantage) |
| `extract_stocks_data`      | Extracts OHLCV data for stock symbols        |
| `extract_crypto_data`      | Extracts OHLCV data for cryptocurrencies     |
| `stream_data_using_kafka`  | Streams raw data into Kafka topic            |
| `transform_stock_data_spark` | Processes and cleans stock data with PySpark |
| `transform_crypto_data_spark` | Processes and cleans crypto data with PySpark |

---

## 🧱 Tech Stack

- **Airflow** – DAG scheduling and orchestration  
- **Kafka** – Real-time streaming layer  
- **PySpark** – Batch data transformation  
- **Docker** – Containerized development environment  
- **PostgreSQL / Delta Lake** – (Pluggable) data storage sinks  
