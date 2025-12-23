## Project Overview
This is an ETL projected inspired Zoomcamp Data Engineering Course
> https://github.com/DataTalksClub/data-engineering-zoomcamp

Docker, Apache Airflow, and Python are used in this project to perform the following tasks:

- Download parquet files from TLC (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Upload to GCS
- Load from GCS to BigQuery Staging Table
- Merge into Main Table
> This project supports analytics engineering on dbt and reporting via SQL and Airflow pipelines

## Table of Contents

- [Prerequisites](#prerequisites)  
- [Quick Start Guide](#quick-start-guide)   
- [Schema Choices](#schema-choices)  
- [Assignments](#assignments)

---

## Prerequisites

To run this project locally, ensure the following tools are installed:

### Docker  
Used to run Airflow and supporting services in isolated containers.

- Download [Docker Desktop](https://www.docker.com/products/docker-desktop)
- After installation, confirm installation via:

```bash
docker --version
docker-compose --version
```

### Git
Used to clone this repository.

- Download [Git](https://git-scm.com/downloads)

### Bash Environment
Required to run shell scripts (e.g., start.sh)

- On macOS/Linux: Bash is preinstalled.
- On Windows: Use one of the following:
    - WSL 2 (recommended) – install via:
```bash
wsl --install
```
    Then use Ubuntu or another Linux distro inside your terminal.

    Git Bash – bundled with Git for Windows.

### Quick Start Guide

1. Clone repository:
```bash
git clone https://github.com/LokalokaC/ny_taxi.git
```

2. Navigate to project: 
```bash
cd ny_taxi
```

3. Run `start.sh`:
```bash
chmod +x start.sh
./start.sh 
#(make sure Docker is running)
```
   > On Windows, use Git Bash or WSL to run shell scripts.
