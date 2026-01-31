# Live Stock Market Data ETL Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
---

## 📌 Project Overview

This project demonstrates the design and implementation of a **real-time stock market data pipeline** real-time stock market data pipeline built with the **Modern Data Engineering Stack**, leveraging Apache Airflow for orchestration, DBT for transformations and data modeling, and Snowflake as the cloud data warehouse. Live market data is continuously streamed from the Real live FinHub API, ingested in real time, and processed through a structured medallion architecture, where raw data is cleaned, enriched, and transformed into analytics-ready datasets. Airflow manages the end-to-end workflow, ensuring reliable scheduling, dependency management, and fault tolerance, while DBT is used to apply scalable transformations, enforce data quality, and produce well-modeled tables optimized for analytics. The final outputs are stored in Snowflake and serve as the foundation for interactive dashboards and visualizations, enabling insights into market trends, price movements, and volatility, and showcasing a complete, production-style data pipeline.

---

## Tools and Stack used
- **Apache Kafka** → Live Market Data Ingestion/Streaming
- **Apache Airflow** → Workflow Orchestration  
- **DBT** → SQL-based Transformations  
- **Python** → API-driven Data Ingestion
- **SQL** → Data transformation in the Medallion Layers 
- **Snowflake** → Cloud Data Warehouse 
- **Docker** → Containerization  

---

## Highlights & Features
- **Live Stock Market Data**Acquisition via The Finhub API
- Real-time streaming pipeline using **Kafka**  
- Orchestrated ETL workflow via **Airflow**  
- Data Transformations using **DBT** inside Snowflake  
- Cloud warehousing using **Snowflake**  

---
**Author**: *Peace KASSA*
