# PowerGrid+: Smart Energy ETL & Real-Time Anomaly Analytics

*A Data Engineering & Analytics Project by **Aneesh AJ***

## 📌 Overview

**PowerGrid+** is an end-to-end **data engineering and analytics
platform** designed to simulate real-time smart grid monitoring.\
It demonstrates the full lifecycle of a modern data pipeline:

-   Synthetic smart-meter generation\
-   Automated ETL processing\
-   Rolling-window feature engineering\
-   Real-time anomaly detection\
-   Storage into PostgreSQL\
-   Interactive Power BI dashboard delivering operational insights

This project simulates how real utilities monitor grid health, detect
abnormal consumption patterns, track meter behavior, and identify
potential faults or energy theft indicators.

It is designed for **recruiters and hiring managers evaluating Data
Engineering candidates.**

------------------------------------------------------------------------

## ⚡ Key Features

-   **Synthetic smart-meter data generator** (configurable,
    multi-region, 336K+ rows)
-   **Python ETL pipeline**
    -   Cleaning, transformations, temporal feature engineering\
    -   Rolling 1-hour load averages\
    -   Power factor + consumption metrics\
-   **Rule-based anomaly detection**
    -   Sudden spikes/drops\
    -   Consumption irregularities\
-   **Gold dataset creation** for analytics\
-   **PostgreSQL integration** (Dockerized)\
-   **Power BI operational dashboard** with:
    -   KPIs\
    -   Load trends\
    -   Region performance\
    -   Hourly heatmap\
    -   Top anomalous meters\
    -   Detailed anomaly review

------------------------------------------------------------------------

## 🏗 Architecture

                    ┌─────────────────────┐
                    │   Data Generation   │
                    │  (Synthetic Meter   │
                    │   Readings - CSV)   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      ETL Layer      │
                    │  Cleaning + Shaping │
                    │  Rolling Features   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Anomaly Detection  │
                    │ (Spike / Drop / PF) │
                    └──────────┬──────────┘
                               │
                               ▼
                      ┌─────────────────┐
                      │   Gold Dataset  │
                      │  (Analytics-     │
                      │   optimized)     │
                      └────────┬────────┘
                               │
                               ▼
                  ┌────────────────────────┐
                  │   PostgreSQL (Docker)  │
                  │  Analytics Warehouse   │
                  └──────────┬────────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │  Power BI Dashboard    │
                   │ Insights & Monitoring  │
                   └────────────────────────┘

------------------------------------------------------------------------

## 🧠 Feature Engineering

The pipeline constructs meaningful operational features:

### ✓ Rolling 1-hour load

    rolling_kw_1h = mean(power_kw over last 4 intervals)

### ✓ Hour of day

Used for hourly heatmaps and temporal patterns.

### ✓ Power factor analysis

Low PF indicates inefficient loads or equipment issues.

### ✓ Percentage load change

Detects sudden transitions:

    kw_pct_change = (current_kw - prev_kw) / prev_kw

------------------------------------------------------------------------

## 🚨 Anomaly Detection Logic

The anomaly engine flags readings using simple rule-based thresholds:

### 1. Sudden Spike

Large upward jump in consumption.

### 2. Sudden Drop

Zero load or rapid decay --- often indicates outages, equipment faults,
or meter resets.

### 3. Continuous Irregularity

Repeated power factor abnormalities.

Each row gets:

-   `anomaly_flag` (True/False)
-   `anomaly_reason` (drop/spike/irregular)

This is optimized for **operational monitoring**, not ML classification.

------------------------------------------------------------------------

## 🗂 Final Gold Dataset Fields

-   timestamp\
-   meter_id\
-   region\
-   voltage\
-   current\
-   power_kw\
-   temperature_c\
-   hour_of_day\
-   rolling_kw_1h\
-   kw_pct_change\
-   anomaly_flag\
-   anomaly_reason

------------------------------------------------------------------------

## 📊 Power BI Dashboard

The dashboard provides real-time operational visibility.

### 1. KPI Summary (Top Row)

-   Total Power Consumption\
-   Average Power Factor\
-   Total Readings Processed\
-   Total Anomalies\
-   Anomaly Percentage

### 2. Regional Performance

Bar chart showing anomaly distribution by region.

### 3. Rolling Load Trend (Line Chart)

Continuous time-series showing how load evolves.

### 4. Hourly Heatmap

Shows consumption patterns over 24 hours × region.

### 5. Top 10 Anomalous Meters

Identifies worst-performing meters.

### 6. Anomaly Table (Drill-down)

Detailed record of flagged anomalies.

------------------------------------------------------------------------

## 🔍 Analytical Insights (Storytelling)

### 🔸 1. North region shows consistently elevated load

North exhibits higher mean consumption and more spikes.\
This may indicate:

-   Industrial clients\
-   Transformer overload\
-   Aging infrastructure

### 🔸 2. Peak consumption occurs between 18:00--21:00

Occurs across all regions --- classic residential demand peak.\
Supports load balancing and peak shaving strategies.

### 🔸 3. Sudden drops are most common anomaly

Often representing outages, equipment faults, or meter resets.

### 🔸 4. Certain meters repeatedly trigger anomalies

Top-10 chart reveals meters with recurring faults.\
These meters should be prioritized for field inspection.

### 🔸 5. Power factor performance indicates inefficiency

PF \< 0.9 in several intervals = potential:

-   Poor load quality\
-   Reactive power issues\
-   Need for capacitor bank adjustments

These insights demonstrate operational value for utilities.

------------------------------------------------------------------------

## 🛠 Tech Stack

### Languages:

Python (pandas, numpy)

### Data Engineering:

-   ETL pipelines\
-   Feature engineering\
-   Anomaly detection\
-   File-based data orchestration

### Database:

PostgreSQL (Dockerized)

### Visualization:

Power BI

### Tools:

Docker, SQLAlchemy, psycopg2

------------------------------------------------------------------------

## 🚀 How to Run the Project

### 1. Start PostgreSQL

    cd db
    docker compose up -d

### 2. Generate Data

    powergrid generate

### 3. Run ETL

    powergrid etl
    powergrid anomalies
    powergrid load

### 4. Load Dashboard

Open **Power BI** → load `gold_dataset.csv` or connect directly to
PostgreSQL.

------------------------------------------------------------------------

## 🔮 Future Improvements

-   Add ML anomaly detection (XGBoost, isolation forest)\
-   Build FastAPI service for real-time prediction\
-   Add Airflow orchestration\
-   Deploy to cloud (AWS RDS + ECS + QuickSight)\
-   Add reactive power + harmonics analysis

------------------------------------------------------------------------

## 👤 Author

**Aneesh AJ**\
Data Engineering & AI Enthusiast
