# 🚀 Ecomm360 – End-to-End Data Engineering Pipeline

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Cloud](https://img.shields.io/badge/Cloud-AWS-orange)
![Processing](https://img.shields.io/badge/Processing-Databricks-red)
![Warehouse](https://img.shields.io/badge/Warehouse-Snowflake-green)
![Orchestration](https://img.shields.io/badge/Orchestration-Airflow-yellow)

---

## 📌 Overview

**SkyLens 360** is a scalable **end-to-end data engineering pipeline** designed to process both **batch and near real-time data** for an e-commerce platform.

The project simulates real-world production systems by implementing:

* **Medallion Architecture (Bronze → Silver → Gold)**
* **Batch + Streaming ingestion**
* **Incremental processing using CDC**
* **Cloud-native data stack**

---

## 🎯 Problem Statement

An e-commerce company needs a **scalable, near real-time analytics platform** to monitor business performance and customer behavior.

The system must:

* Simulate **real-time ingestion** using historical data
* Handle **incremental updates & schema evolution**
* Transform normalized datasets into a **denormalized analytical model**
* Support **high-performance queries on 1M+ records**

---

## 🎯 Project Goals

### 📊 Business KPIs

* Revenue by state/region
* Delivery delay metrics
* Customer Lifetime Value (CLV)

### 🔍 Advanced Analytics

* Cohort analysis across customers, orders, payments, and reviews

### ⚡ Performance

* Low-latency BI queries
* Optimized storage and compute

### 🏗️ Engineering Goals

* Scalable pipeline design
* Incremental processing (CDC MERGE)
* Star schema modeling

---

## 🏗️ Architecture Overview

### 🔹 High-Level Flow

```
Batch CSV → S3 (staging → raw)
Live Data → Lambda → S3 (live)

        ↓

Databricks (PySpark)
Bronze → Silver → Gold

        ↓

Snowflake (Warehouse)

        ↓

Streamlit Dashboard
```

---

## ☁️ Data Lake – Amazon S3

| Folder   | Purpose                  |
| -------- | ------------------------ |
| staging/ | Raw uploaded batch files |
| raw/     | Cleaned ingestion layer  |
| live/    | Streaming data           |

---

## ⚡ Processing – Databricks

### 🧱 Medallion Architecture

#### 🥉 Bronze

* Raw ingestion from S3
* No transformation

#### 🥈 Silver

* Data cleaning & joins
* Handles **CDC MERGE (incremental updates)**

#### 🥇 Gold

* Business-level aggregations
* Fact + dimension tables

---

## 🔄 Data Pipeline

### 📥 Batch Pipeline

* Manual CSV upload → S3 staging
* Airflow detects files
* Moves to raw layer
* Triggers Databricks notebooks

---

### ⚡ Live Pipeline

* EventBridge triggers every 10 minutes
* Lambda generates synthetic orders
* Writes data to S3 live
* Airflow processes incrementally

---

## 📦 4-Batch Strategy

| Batch | Content                    | Trigger        |
| ----- | -------------------------- | -------------- |
| 1     | All dimensions + 25% facts | Manual         |
| 2     | 25% facts                  | Airflow (T+0)  |
| 3     | 25% facts                  | Airflow (T+20) |
| 4     | 25% facts                  | Airflow (T+40) |

### ✅ Why This Works

* Enables full pipeline validation
* Simulates real-world incremental loads
* Ensures CDC logic correctness

---

## 🔗 Airflow Orchestration

Runs on EC2 using **Apache Airflow**

### DAGs

**1. Batch Pipeline**

* Batch 1 → Manual
* Batch 2–4 → Scheduled

**2. Live Pipeline**

* Runs every 15 minutes

---

### ⚙️ Databricks Integration

Uses:

```
DatabricksSubmitRunOperator
```

* Direct notebook execution
* No pre-created jobs needed
* Works with free/serverless Databricks

---

## 🔀 Batch vs Live Merge

| Layer     | Operation    |
| --------- | ------------ |
| Batch 1   | OVERWRITE    |
| Batch 2–4 | MERGE        |
| Live      | MERGE        |
| Gold      | FULL REBUILD |

---

## 🧊 Data Warehouse – Snowflake

* Star Schema Design:

  * Fact tables
  * Dimension tables
  * Aggregated tables

---

## 📊 KPIs & Analytics

### 💰 Revenue by State

```sql
SELECT state, SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY state;
```

---

### 🚚 Delivery Delay

```sql
SELECT AVG(delivery_delay) AS avg_delay
FROM fact_orders;
```

---

### 👤 Customer Lifetime Value (CLV)

```sql
SELECT customer_id, SUM(total_amount) AS clv
FROM fact_orders
GROUP BY customer_id;
```

---

## 📊 Dashboard (Streamlit)

Displays:

* Revenue trends
* Customer analytics
* Delivery performance
* Seller insights

---

## 📁 Project Structure

```
├── airflow/
├── databricks/
├── notebooks/
├── sql/
├── streamlit_app/
├── data/
└── README.md
```

---

## ▶️ How to Run

### 1. Clone Repo

```bash
git clone https://github.com/your-username/skylens360.git
cd skylens360
```

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 3. Configure AWS + Snowflake + Databricks

* Add credentials in config files
* Set up S3 buckets
* Configure Airflow connections

---

### 4. Start Airflow

```bash
airflow standalone
```

---

### 5. Trigger Pipeline

* Run Batch 1 manually
* Airflow schedules remaining batches

---

## 💰 Cost Optimization

| Service     | Cost       |
| ----------- | ---------- |
| S3          | Free Tier  |
| EC2         | ~$15/month |
| Databricks  | Free       |
| Snowflake   | Free Trial |
| Lambda      | Free       |
| EventBridge | Free       |
| SNS         | Free       |

**Total Cost: ~ $15/month**

---

## 🚀 Key Highlights

* End-to-end pipeline (Batch + Streaming)
* Real-world architecture (Medallion + CDC)
* Scalable & cost-efficient
* Production-like orchestration
* Handles 1M+ records efficiently

---

