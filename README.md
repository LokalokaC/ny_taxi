# 🚕 NYC Taxi Infrastructure: Automated Cloud Data Pipeline

[![Infrastructure](https://img.shields.io/badge/Infrastructure-GCP-blue?style=flat-square&logo=google-cloud)](https://cloud.google.com/)
[![Orchestration](https://img.shields.io/badge/Orchestrator-Airflow-red?style=flat-square&logo=apache-airflow)](https://airflow.apache.org/)
[![Workflow](https://img.shields.io/badge/Workflow-Docker%20%26%20CI%2FCD-green?style=flat-square&logo=github-actions)](https://github.com/features/actions)

## 📌 Project Overview
This project focuses on building a robust, automated **Data Ingestion Infrastructure**. It orchestrates the extraction of raw New York City Taxi & Limousine Commission (TLC) data, processes it via Python, and loads it into **Google BigQuery**.

This repository represents the **Upstream Pipeline (EL)** in my data architecture. By leveraging cloud-native tools, I ensured that the data is ready for downstream analytics.

> 💡 **Looking for the Transformation layer?**
> The data modeling and dbt transformation logic are hosted in a separate repository: [dbt-analytics](https://github.com/LokalokaC/dbt-analytics)

---

## 🏗️ System Architecture
The pipeline is deployed on Google Cloud Platform, simulating a production-grade environment:

1.  **Extract**: Python scripts fetch Parquet files from the TLC source.
2.  **Orchestrate**: **Apache Airflow** manages the DAGs, task dependencies, and retry logic.
3.  **Containerize**: **Docker** & **Docker-compose** ensure environment consistency across local development and the GCP VM.
4.  **Cloud Storage**: **Google Cloud Storage (GCS)** acts as the Data Lake for landing and staging files.
5.  **Data Warehouse**: Structured data is loaded into **BigQuery**, utilizing partitioning for cost-efficiency.
6.  **CI/CD Pipeline**: 
    * **GitHub Actions** triggers on every push to the main branch.
    * Automatically builds Docker images and pushes them to **GCP Artifact Registry**.
    * Streamlines deployment to the **Compute Engine (VM)**.

---

## 🛠️ Tech Stack
* **Cloud Platform**: Google Cloud Platform (GCE, GCS, BigQuery, Artifact Registry)
* **Orchestration**: Apache Airflow
* **Infrastructure & DevOps**: Docker, GitHub Actions, Bash Scripting
* **Languages**: Python (PyArrow/Pandas), SQL

---

## 🚀 Quick Start Guide

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* [Git](https://git-scm.com/downloads)
* A GCP Service Account JSON key (with Storage and BigQuery Admin permissions)

### 2. Local Setup
```bash
# Clone the repository
git clone [https://github.com/LokalokaC/ny_taxi.git](https://github.com/LokalokaC/ny_taxi.git)
cd ny_taxi