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
| **Storage** | Local filesystem (S3-compatible paths supported) |
| **Testing** | Pytest |
| **Monitoring / Quality** | Airflow alerts, Great Expectations (optional) |
| **Containerization** | Docker + docker-compose |


## 🏗️ Architecture Overview

```mermaid
flowchart TD
    A[Open Brewery API] -->|Fetch JSON| B[Bronze Layer 🥉<br>Raw data JSON files]
    B -->|Clean, Normalize| C[Silver Layer 🥈<br>Partitioned CSV/Parquet by Location]
    C -->|Aggregate Metrics| D[Gold Layer 🥇<br>Aggregated KPIs: Breweries by Type & Location]
    D -->|Expose for Analytics| E[BI Tools / Dashboards]



## Pipeline Orchestration (Airflow DAG)

The pipeline is orchestrated by Apache Airflow, scheduled to run daily.

flowchart LR
    F[fetch_bronze<br>Fetch API Data] --> T[transform_silver_gold<br>Clean & Aggregate]


Retries: Configured with exponential backoff

Failure callback: Logs detailed errors

Email alert: Sent on task failure

Parallelism: Only 1 active run at a time to prevent race conditions

## 🧩 Project Structure
bees-breweries-case/
├── dags/
│   ├── openbrewery_dag.py        # Airflow DAG definition
│   ├── fetcher.py                # API ingestion logic (Bronze)
│   ├── transformer.py            # Transformation logic (Silver & Gold)
├── src/
│   └── tests/
│       ├── test_fetcher.py       # Unit tests for fetcher
│       └── test_transformer.py   # Unit tests for transformer
├── docker-compose.yml            # Airflow + Postgres environment
├── requirements.txt              # Dependencies
└── README.md                     # Documentation (this file)

## 🧪 Testing

Tests are written using pytest.

Running Tests

To run unit tests inside Docker:

docker-compose run --rm pytest

This executes all tests inside src/tests.

Example test
def test_write_bronze_creates_file():
    data = [{"id": "1", "name": "Test Brewery"}]
    tmpdir = tempfile.mkdtemp()
    path = write_bronze(data, tmpdir)
    assert os.path.exists(path)
    with open(path, "r") as f:
        loaded = json.load(f)
    assert loaded == data


## 🚀 How to Run Locally

### 1️⃣ Prerequisites
- Docker & docker-compose installed  
- At least 4 GB RAM allocated to Docker
- Ports 8080 (Airflow UI) and 4040 (Spark UI) available

Clone the repo
git clone https://github.com/<your-username>/bees-breweries-case.git
cd bees-breweries-case


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

## 🔍 Monitoring & Alerting

### 🧩 Failures

Airflow handles retries and backoff (retries=2, retry_delay=5min)

Email alerts configured via ALERT_EMAIL environment variable

Failed tasks logged in /opt/airflow/logs

### 📊 Data Quality

Add a validation task (future improvement):

Check for schema drift (columns missing)

Validate non-null brewery names

Detect duplicates by brewery_id

### 📈 Observability

You can extend monitoring by integrating:

Prometheus + Grafana for task metrics

Sentry or Datadog for alerting

Great Expectations for data validation in Silver/Gold layers

### ☁️ Cloud or Local Deployment

You can run the pipeline:

Locally via Docker Compose (default)

On the cloud (GCP/AWS/Azure) with:

Airflow on Cloud Composer / MWAA

Data stored in S3 / GCS instead of local folders

Use environment variables for paths (e.g., BRONZE_DIR, SILVER_DIR, GOLD_DIR)

### 🚀 Future Improvements

Switch Silver/Gold outputs to Parquet (for columnar efficiency)

Add partitioning by state in Silver layer

Integrate Great Expectations for data validation

Deploy Airflow on Kubernetes for scalability

Add CI/CD pipeline (GitHub Actions) for automated testing

### 👩‍💻 Author

Marina Vinhaes
📧 marinalvinhaes@gmail.com 