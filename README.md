# Rider Incentives Data Pipeline

## Structure

rider-incentives-data-pipeline/ 
├── src/ 
│   └── rider-incentives-data-pipeline.py 
├── LICENSE
│
├── README.md 
│
└── requirements.txt
 
## Overview
This project simulates a real-world data engineering workflow for a delivery platform. \
It ingests rider performance data and promotional codes from external APIs, applies business logic to determine eligibility and tier classification, assigns incentives, and triggers notifications via webhook.\

The goal is to demonstrate an end-to-end data pipeline, including data ingestion, transformation, and operational output.

---

## Architecture

External APIs (JSON) \
        ↓ \
Data Ingestion (requests) \
        ↓ \
Data Validation & Cleaning
        ↓ \
Business Logic (Eligibility + Tiering) \
        ↓ \
Promo Code Allocation \
        ↓ \
Webhook Notification (POST request) 

---

## Tech Stack

- **Python**
- **REST APIs**
- **JSON data processing**
- **ETL pipeline logic**
- **Webhooks (event-driven architecture)**

---

## Key Features

- Ingests data from multiple external sources (rider data and promo codes)
- Validates and cleans incoming data
- Applies business rules to determine rider eligibility
- Classifies riders into performance tiers (Platinum, Gold, Silver, Bronze)
- Assigns promo codes dynamically based on city and tier
- Sends automated notifications via HTTP POST requests
- Handles missing data and edge cases gracefully

---

## Business Logic

### Eligibility Criteria
A rider is eligible if:
- Status is **active**
- Works **≥ 25 hours/week**
- Completes **≥ 50 orders/week**
- Acceptance rate **≥ 88%**
- Reassignment rate **≤ 5%**
- Customer rating **≥ 4.75**
- Average delivery time **≤ 30 minutes**

### Tier Classification
- **Platinum** → >90 orders & rating ≥ 4.9  
- **Gold** → >75 orders & rating ≥ 4.8  
- **Silver** → >60 orders  
- **Bronze** → all other eligible riders  

---

## How to Run

### Prerequisites

- Python 3.8+
- pip

1. Clone the repository:
   
```bash
git clone https://github.com/nirevexin/rider-incentives-data-pipeline
cd rider-incentives-data-pipeline
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```
3. Run the pipeline:

```bash
python src/rider-incentives-data-pipeline.py
```


##  Example Output
- Total riders processed
- Number of eligible riders
- Number of notifications sent
- Logs for missing data or unavailable promo codes

## What This Project Demonstrates
- End-to-end ETL pipeline design
- Data validation and transformation
- Implementation of business logic in data workflows
- Resource allocation strategies (promo codes)
- Integration with external systems via APIs
- Clean and modular Python code structure

## Possible Improvements
- Store processed data in a data warehouse (Redshift, Snowflake)
- Use Airflow for orchestration
- Persist data in Parquet format on S3
- Add unit tests and CI/CD pipeline
- Replace webhook with message queue (Kafka / SQS)

## Author
- Alexey Vershinin Dudin
- Data Analyst / Data Engineer
- Barcelona, Spain    
