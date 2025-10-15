# bees-breweries-case

# 🍺 Open Brewery Medallion Data Pipeline

This repository implements a **data engineering pipeline** that consumes the [Open Brewery DB API](https://www.openbrewerydb.org/), transforms and persists data into a **Data Lake following the Medallion Architecture** (Bronze → Silver → Gold), and orchestrates everything using **Apache Airflow**.

---

## 🧱 Architecture Overview

**Goal:** Fetch brewery data, store it in a raw layer (Bronze), transform/clean it into a structured columnar format (Silver), and create analytical aggregates (Gold).

### 🏗️ Medallion Layers

| Layer | Description | Format | Partitioning |
|:------|:-------------|:--------|:--------------|
| **Bronze** | Raw API JSON data | JSON | Timestamped |
| **Silver** | Cleaned, normalized data | Parquet | `state` |
| **Gold** | Aggregated view (count of breweries per type and state) | Parquet | None |

---

## 🧰 Tech Stack

| Component | Technology |
|:-----------|:------------|
| **Language** | Python 3.10 |
| **Orchestration** | Apache Airflow 2.8 |
| **Transformations** | PySpark 3.4 |
| **Storage** | Local filesystem (S3-compatible paths supported) |
| **Testing** | Pytest |
| **Monitoring / Quality** | Airflow alerts, Great Expectations (optional) |
| **Containerization** | Docker + docker-compose |
| **Optional Cloud** | AWS S3, Databricks/EMR for Spark, MWAA for Airflow |

## 🚀 How to Run Locally

### 1️⃣ Prerequisites
- Docker & docker-compose installed  
- At least 4 GB RAM allocated to Docker
- Ports 8080 (Airflow UI) and 4040 (Spark UI) available

---

### 2️⃣ Build and start Airflow + Spark

From the project root:

1️⃣ Start all services:

docker-compose up -d


2️⃣ Initialize Airflow database:

docker exec -it bees-breweries-case-airflow-webserver airflow db init


3️⃣ Access the Airflow UI:

🌐 Open: http://localhost:8080

Default login: airflow / airflow

4️⃣ Trigger the DAG:
Once the webserver is up, enable & trigger openbrewery_medallion.

Running Tests

To run unit tests inside Docker:

docker-compose run --rm pytest

This executes all tests inside src/tests.