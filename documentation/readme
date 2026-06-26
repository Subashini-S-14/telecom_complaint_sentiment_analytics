# 📊 Telecom Complaint Data Platform

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=flat-square&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.1.0-150458?style=flat-square&logo=pandas&logoColor=white)
![YouTube API](https://img.shields.io/badge/YouTube%20Data%20API-v3-FF0000?style=flat-square&logo=youtube&logoColor=white)
![FCC](https://img.shields.io/badge/FCC-Open%20Data%20API-003087?style=flat-square&logo=databricks&logoColor=white)
![License](https://img.shields.io/badge/License-Educational-green?style=flat-square)

A production-grade, end-to-end **Big Data pipeline** that ingests, processes, and visualizes telecom complaint data from the **FCC Open Data API** and **YouTube Data API v3**. Built on a **Medallion Architecture** (Bronze → Silver → Gold → Warehouse) orchestrated by **Apache Airflow** and powered by **PySpark**, all containerized with **Docker**.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│         FCC Open Data API          YouTube Data API v3              │
└───────────────┬─────────────────────────┬───────────────────────────┘
                │                         │
                ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw Ingestion)                    │
│    data_lake/bronze/telecom_complaints/   (JSON files, paginated)   │
│    data_lake/bronze/youtube_sentiment/    (JSON files, by query)    │
└───────────────────────────────┬─────────────────────────────────────┘
                                │  Apache Airflow (Daily Schedule)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleaned & Typed)                  │
│    data_lake/silver/telecom_complaints/   (Parquet, partitioned)    │
│    data_lake/silver/youtube_sentiment/    (Parquet, partitioned)    │
└───────────────────────────────┬─────────────────────────────────────┘
                                │  PySpark (via Docker exec)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER   (Aggregated for Analytics)          │
│  complaints_by_region  |  service_issue_trends  |  daily_trend      │
│  service_distribution  |  youtube_trends  |  youtube_top_channels   │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    WAREHOUSE    (Star Schema)                        │
│  dim_location  |  dim_service  |  dim_time  |  dim_channel          │
│  fact_complaints  |  fact_youtube_sentiment                          │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              STREAMLIT DASHBOARD  (Interactive Analytics)           │
│      FCC Complaints Tab  |  YouTube Sentiment Tab                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
Telecom-complaint-data-platform/
│
├── airflow/
│   └── dags/
│       └── telecom_pipeline.py       # Airflow DAG (daily orchestration)
│
├── data_ingestion/
│   ├── complaints_api_extract.py     # FCC Open Data paginated ingestion
│   ├── youtube_api_extract.py        # YouTube Data API v3 ingestion
│   ├── offset.txt                    # FCC ingestion resumption cursor
│   ├── ingestion.log                 # FCC ingestion log
│   └── youtube_ingestion.log         # YouTube ingestion log
│
├── processing/
│   ├── silver_transform.py           # FCC: Bronze → Silver (PySpark)
│   ├── silver_youtube_transform.py   # YouTube: Bronze → Silver (PySpark)
│   ├── gold_aggregation.py           # FCC: Silver → Gold (PySpark)
│   ├── gold_youtube_aggregation.py   # YouTube: Silver → Gold (PySpark)
│   └── star_schema.py                # Gold → Warehouse star schema (PySpark)
│
├── data_lake/
│   ├── bronze/
│   │   ├── telecom_complaints/       # Raw FCC JSON files
│   │   └── youtube_sentiment/        # Raw YouTube JSON files
│   ├── silver/
│   │   ├── telecom_complaints/       # Cleaned FCC Parquet (year/month partitioned)
│   │   └── youtube_sentiment/        # Cleaned YouTube Parquet (year/month partitioned)
│   ├── gold/
│   │   ├── complaints_by_region/
│   │   ├── service_issue_trends/
│   │   ├── service_distribution/
│   │   ├── daily_trend/
│   │   ├── youtube_trends/
│   │   ├── youtube_top_channels/
│   │   └── youtube_recent_videos/
│   └── warehouse/
│       ├── dim_location/
│       ├── dim_service/
│       ├── dim_time/
│       ├── dim_channel/
│       ├── fact_complaints/
│       └── fact_youtube_sentiment/
│
├── dashboard/
│   └── app.py                        # Streamlit dashboard
│
├── Dockerfile                        # PySpark engine container image
├── docker-compose.yml                # Orchestrates Spark + Airflow containers
├── config.yaml                       # Centralized API and pipeline configuration
├── requirements.txt                  # Python dependencies
├── run.sh                            # Startup helper script
├── .gitignore
└── .dockerignore
```

---

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with WSL2 backend on Windows)
- Python 3.11+
- A **YouTube Data API v3** key (from [Google Cloud Console](https://console.cloud.google.com/))

### 1. Clone the Repository

```bash
git clone https://github.com/siddharth99520/Telecom-complaint-data-platform.git
cd Telecom-complaint-data-platform
```

### 2. Set the YouTube API Key

The YouTube extractor reads the key from an environment variable. Set it before running:

```powershell
# PowerShell (Windows)
$env:YOUTUBE_API_KEY = "your_youtube_api_key_here"
```

```bash
# Bash/Linux
export YOUTUBE_API_KEY="your_youtube_api_key_here"
```

### 3. Start the Docker Services

```bash
docker-compose up --build -d
```

This starts two containers:
| Container | Image | Role |
|---|---|---|
| `telecom_spark_engine` | Custom (Python 3.11 + PySpark) | Executes all processing scripts |
| `telecom_airflow` | `apache/airflow:2.9.3` | Orchestrates the daily pipeline |

The Airflow UI will be available at **http://localhost:8080** (credentials: `admin` / `admin`).

### 4. Run Data Ingestion (Bronze Layer)

These scripts run **outside** Docker on your local machine or on a schedule:

```bash
# Ingest FCC telecom complaints (paginated, resumable)
python data_ingestion/complaints_api_extract.py

# Ingest YouTube sentiment data
python data_ingestion/youtube_api_extract.py
```

### 5. Run the Airflow Pipeline

Once the Bronze layer has data, trigger the DAG from the Airflow UI, or manually run the processing steps:

```bash
# Silver layer
docker exec telecom_spark_engine python /workspace/processing/silver_transform.py
docker exec telecom_spark_engine python /workspace/processing/silver_youtube_transform.py

# Gold layer
docker exec telecom_spark_engine python /workspace/processing/gold_aggregation.py
docker exec telecom_spark_engine python /workspace/processing/gold_youtube_aggregation.py

# Warehouse (Star Schema)
docker exec telecom_spark_engine python /workspace/processing/star_schema.py
```

### 6. Launch the Dashboard

Install dashboard dependencies locally (if not already):

```bash
pip install streamlit plotly pandas pyarrow
```

Run the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```

---

## ⚙️ Configuration (`config.yaml`)

All pipeline parameters are centralized in `config.yaml`:

```yaml
youtube:
  api:
    service_name: youtube
    version: v3
  queries:
    - telecom complaints india
    - mobile network issues
    - internet outage complaints
    - broadband problems india
    - telecom service issues
  max_results: 50          # Results per page
  total_pages: 5           # Pages to fetch per query
  region_code: IN          # Target region (India)
  language: en
  output:
    bronze_path: data_lake/bronze/youtube_sentiment
  throttling:
    base_sleep: 1          # Seconds between requests
    retry_attempts: 3      # Retries on failure (exponential backoff)

fcc:
  api_url: https://opendata.fcc.gov/resource/3xyp-aqkj.json
  limit: 1000              # Records per page
  output:
    bronze_path: data_lake/bronze/telecom_complaints
  throttling:
    base_sleep: 1
    retry_attempts: 3
```

---

## 🔄 Pipeline Details

### Airflow DAG: `telecom_data_pipeline`

**Schedule:** `@daily` | **Max Active Runs:** 1 | **Retries:** 1 (5-min delay)

```
bronze_ingestion (placeholder)
        │
        ├──► silver_fcc_processing ──► gold_fcc_processing ──┐
        │                                                      ├──► warehouse_processing
        └──► silver_youtube_processing ──► gold_youtube_processing ──┘
```

| Task ID | Description |
|---|---|
| `bronze_ingestion` | Placeholder — ingestion is run externally |
| `silver_fcc_processing` | Cleans & types raw FCC complaints into Parquet |
| `silver_youtube_processing` | Cleans & types raw YouTube data into Parquet |
| `gold_fcc_processing` | Aggregates FCC data into 4 analytical datasets |
| `gold_youtube_processing` | Aggregates YouTube data into 3 analytical datasets |
| `warehouse_processing` | Builds the full star schema from Silver layers |

---

## 🧱 Data Lake Layers

### Bronze — Raw Data

| Source | Format | Location |
|---|---|---|
| FCC Complaints | JSON (paginated) | `data_lake/bronze/telecom_complaints/complaints_YYYYMMDD_HHMMSS_offset_N.json` |
| YouTube | JSON (array) | `data_lake/bronze/youtube_sentiment/youtube_YYYYMMDD_HHMMSS.json` |

### Silver — Cleaned & Typed

**FCC Complaints** (`silver_transform.py`):
- Drops rows with null `id`, `issue_type`, or `state`
- Deduplicates on `complaint_id`
- Parses timestamps (`ticket_created`, `date_created`)
- Normalizes text fields (`UPPER TRIM`)
- Casts lat/long to `DoubleType`
- Partitioned by `year` / `month` (4 Parquet files per partition)

**YouTube Data** (`silver_youtube_transform.py`):
- Drops rows with null `video_id`
- Deduplicates on `video_id`
- Parses `published_at` to timestamp
- Trims whitespace from text fields; uppercases `query`
- Handles gracefully if no Bronze data exists (exits cleanly)
- Partitioned by `year` / `month` (2 files per partition)

### Gold — Analytical Aggregations

**FCC Complaints** (`gold_aggregation.py`):

| Dataset | Description |
|---|---|
| `complaints_by_region` | Total complaints grouped by `state` + `city` |
| `service_issue_trends` | Complaint counts by `issue` + `year` + `month` |
| `service_distribution` | Complaint counts by `issue_type` |
| `daily_trend` | Daily complaint volume over time |

**YouTube Sentiment** (`gold_youtube_aggregation.py`):

| Dataset | Description |
|---|---|
| `youtube_trends` | Video counts by `query` + `year` + `month` |
| `youtube_top_channels` | Most active channels by video count |
| `youtube_recent_videos` | Latest 50 videos for the dashboard feed |

### Warehouse — Star Schema

**Fact Tables:**

| Table | Grain | Foreign Keys |
|---|---|---|
| `fact_complaints` | One row per complaint | `location_id`, `service_id`, `date_id` |
| `fact_youtube_sentiment` | One row per video | `channel_id`, `date_id` |

**Dimension Tables:**

| Table | Columns |
|---|---|
| `dim_location` | `location_id` (MD5), `state`, `city`, `zip` |
| `dim_service` | `service_id` (MD5), `issue_type`, `method` |
| `dim_time` | `date_id` (YYYYMMDD int), `date`, `year`, `month`, `day` |
| `dim_channel` | `channel_id` (MD5 of channel_title), `channel_title` |

> **Surrogate keys** are generated using MD5 hashes of business keys via PySpark's `F.md5()` with `coalesce` on nullable fields to prevent hash instability.

---

## 📊 Dashboard (`dashboard/app.py`)

Built with **Streamlit** + **Plotly**. Reads directly from the Gold layer Parquet files.

**Key Features:**
- 🔢 **Animated KPI cards** — total complaints, affected states, and top issue type count up from zero using a JavaScript `requestAnimationFrame` easing function
- 📦 **5-minute cache** via `@st.cache_data(ttl=300)` to avoid re-reading Parquet on every interaction
- 🏛️ **FCC Complaints Tab:**
  - Top 10 states by complaint volume (horizontal bar chart)
  - Service type distribution (donut chart)
  - Daily complaint trend (area chart)
  - Monthly issue breakdown (stacked bar chart)
  - Sidebar filter: drill down by state
- ▶️ **YouTube Sentiment Tab:**
  - Monthly video mentions by query (grouped bar chart)
  - Top reporting YouTube channels (horizontal bar chart)
  - Recent 50 videos feed (data table)
- Graceful fallback if YouTube data is unavailable

---

## 🐳 Docker Setup

### `Dockerfile` (Spark Engine)

```
Base:      python:3.11-slim
JDK:       default-jdk (required for PySpark/JVM)
Extras:    curl, wget, Kafka-Spark connector JAR
Deps:      requirements.txt (pandas, pyspark, delta-spark, etc.)
Workdir:   /workspace (mapped to project root)
```

### `docker-compose.yml`

| Service | Container | Key Config |
|---|---|---|
| `spark_engine` | `telecom_spark_engine` | Mounts project root to `/workspace`; stays alive via `tail -f /dev/null` |
| `airflow` | `telecom_airflow` | Airflow 2.9.3; SequentialExecutor; DAGs mounted from `./airflow/dags`; Docker socket mounted for `docker exec` calls |

> Airflow runs as `root` briefly to install Docker CLI and fix `/var/run/docker.sock` permissions, then drops to `airflow` user via `su -p airflow`.

---

## 📦 Dependencies

| Package | Version | Purpose |
|---|---|---|
| `pandas` | 2.1.0 | Dashboard data manipulation |
| `requests` | 2.31.0 | FCC API HTTP client |
| `pyyaml` | 6.0.1 | Config file parsing |
| `pyspark` | 3.5.0 | Distributed data processing |
| `delta-spark` | 3.1.0 | Delta Lake support (future use) |
| `streamlit` | latest | Interactive web dashboard |
| `matplotlib` | latest | Supplementary plotting |
| `google-api-python-client` | (transitive) | YouTube Data API v3 client |

---

## 🔑 Environment Variables

| Variable | Required | Description |
|---|---|---|
| `YOUTUBE_API_KEY` | ✅ Yes | Google Cloud API key with YouTube Data API v3 enabled |

---

## 📐 Design Decisions

| Decision | Rationale |
|---|---|
| **Medallion Architecture** | Clear separation of concerns: raw → clean → aggregated → analytical |
| **Parquet + partitioning** | Columnar storage with partition pruning for fast analytical queries |
| **`coalesce` over `repartition`** | Reduces output file count without triggering an extra shuffle stage |
| **MD5 surrogate keys** | Deterministic, hash-based keys avoid auto-increment issues in distributed writes |
| **`docker exec` from Airflow** | Airflow orchestrates but delegates all Spark execution to the dedicated Spark container |
| **Offset-based FCC ingestion** | Resumable pagination — survives interruptions and avoids re-fetching data |
| **Exponential backoff** | Handles transient API failures gracefully without overwhelming upstream APIs |
| **`@st.cache_data(ttl=300)`** | Prevents redundant Parquet I/O on every Streamlit widget interaction |

---

## 🛠️ Troubleshooting

**Airflow cannot connect to Docker socket:**
```bash
# Ensure Docker socket permissions are correct
chmod 666 /var/run/docker.sock
```

**`YOUTUBE_API_KEY` not found error:**
```bash
# Verify the variable is set in the current shell
echo $YOUTUBE_API_KEY   # Bash
$env:YOUTUBE_API_KEY    # PowerShell
```

**Gold layer Parquet not found (Dashboard error):**
- Ensure both Silver layers are populated before running Gold scripts
- Run scripts in order: Silver → Gold → Warehouse

**PySpark `OutOfMemoryError`:**
- Reduce `spark.sql.shuffle.partitions` (already tuned to 8/4 for local runs)
- Ensure Docker Desktop has at least **4 GB RAM** allocated

---

## 📜 License

This project is for educational purposes only.

---

*Built with ❤️ using PySpark, Apache Airflow, Docker, and Streamlit.*
