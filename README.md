# 🛍️ Databricks Retail Streaming ETL (Declarative DLT + Kimball)

This project implements a **real-time ETL pipeline** for retail data using **Databricks Delta Live Tables (DLT)** with a **declarative pipeline approach**.  
Data flows continuously from raw streaming sources into the **Medallion Architecture** (Bronze → Silver → Gold), where the Gold layer is structured into a **Kimball Star Schema** (facts & dimensions) for analytics.

---

## 🚀 Features
- **Declarative pipeline** with Databricks DLT (`@dlt.table`, `@dlt.view`) – no manual DAG orchestration needed.  
- **Streaming ingestion** of retail data.  
- **Data quality enforcement** via DLT `EXPECTATIONS`.
- **Data quarantine for records that fail quality checks**, ensuring bad data is isolated without breaking the pipeline.
- **Medallion architecture** (Bronze → Silver → Gold) for incremental data quality.  
- **Gold layer = Kimball Star Schema** (Fact & Dimension tables).  
- Automatic **lineage tracking, retries, and monitoring** by Databricks.  

---

## 🏗️ Architecture
Source (Streaming)
→ Bronze (Raw Stream)
→ Silver (Cleansed, standardized, deduplicated)
→ Gold (Star Schema: Fact + Dimensions)

---

## 📐 Kimball Star Schema (Gold Layer)
The Gold layer is modeled into a Kimball Star Schema using the Yugabyte Retail Analytics dataset:
- Fact Tables
  - `fact_orders` → order-level transactions (order_id, user_id, product_id, quantity, total_amount, discount, keys to dimensions).
  - `fact_reviews` → customer reviews (review_id, user_id, product_id, rating, review_date).
- Dimension Tables
  - `dim_users` → customer profiles (user_id, name, email, location, signup_date).
  - `dim_products` → product catalog (product_id, name, category, brand, price).

This star schema supports sales analysis, customer behavior insights, and product performance tracking.

Link to sample dataset: [Yugabyte Retail Analytics Sample Dataset](https://docs.yugabyte.com/preview/sample-data/retail-analytics/)

---

## ⚙️ Tech Stack
- Databricks Delta Live Tables (DLT) → streaming + declarative ETL pipelines
- Declarative Pipeline → automatic orchestration, lineage, and quality checks (`@dlt.table`, `EXPECTATIONS`)
- PySpark → transformations & business logic inside DLT
- Kimball Data Warehousing Methodology → Gold layer modeled as fact & dimension tables (Star Schema)
