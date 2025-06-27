# Data Ingestion Pipeline: PurpleAir to AWS S3

##  Project Overview

This project establishes an automated, robust data pipeline to collect real-time air quality data from a **PurpleAir sensor** via its API. The data is then transformed, optimized, and stored in an **AWS S3-based data lake**.

The current data source is a pre-set PurpleAir sensor located in Ankara, Turkey. In the future, this will serve as the foundation for building a comprehensive air quality monitoring and analysis platform, demonstrating capabilities in cloud data ingestion, storage optimization, and cross-cloud integration.

### Key Features:

* **Automated Data Ingestion:** Scheduled collection of air quality data.
* **Robust Error Handling:** Designed to gracefully manage API call failures and sensor data anomalies.
* **Data Transformation & Validation:** Cleanses, unpacks, and validates raw sensor readings.
* **Optimized Data Storage:** Stores data in AWS S3 using the efficient **Parquet format** and **time-based partitioning**.

## 💡 My Learning Journey

Having previously gained experience simulating environmental data and loading it into Google Cloud Platform (GCP), I embarked on this project to gain hands-on experience with:

* **Another major cloud provider (AWS):** Setting up core data infrastructure from scratch.
* **Real-world API integration:** Connecting to the PurpleAir API to fetch live data.
* **Robust Python development for cloud environments:** Deepening skills in `requests`, `pandas`, `boto3`, and `pyarrow`.
* **Mastering AWS Lambda Layers:** Overcoming significant challenges in packaging and deploying complex Python libraries with native components (like `pyarrow`) to Lambda's Linux runtime environment. This was a particularly valuable and challenging learning experience, involving understanding environment variables, timeouts, memory, and debugging deployment issues.
* **Implementing Data Lake Best Practices:** Gaining practical experience with columnar storage (Parquet) and data partitioning for optimized querying and cost efficiency.

## 🏗️ Project Architecture and Data Flow

The pipeline is designed for scheduled execution, ensuring continuous collection of air quality and associated environmental data.

### Data Collected:

* **Sensor Metadata:** `last_seen`, `rssi`
* **Environmental Readings:** `humidity`, `temperature`, `pressure`, `pm2.5` (various readings: `pm2.5_atm`, `pm2.5_alt`, `pm2.5_6hour`), `visual_range`

### High-Level Data Flow:

1.  **Scheduled Trigger:** An AWS EventBridge Rule triggers the AWS Lambda function every 6 hours.
2.  **Data Fetch (`get_data`):** The Lambda function makes an API call to PurpleAir, retrieves raw sensor data. It includes robust error handling to manage network issues or API failures.
3.  **Sensor Freshness Check (`check_sensor_freshness`):** Verifies if the sensor's `last_seen` timestamp is within an acceptable delay (e.g., 10 minutes) of the current time, indicating the sensor is actively transmitting data.
4.  **Data Transformation & Validation (`clean_and_validate_sensor_data`):** Unpacks and cleans the raw JSON response, validates data types, handles potential missing values, and prepares it into a structured Pandas DataFrame.
5.  **Data Enrichment:** Adds `ingestion_timestamp_iso_utc` and `ingestion_timestamp_unix_utc` columns to track when the data was ingested into the pipeline.
6.  **Data Storage (`lambda_handler`):** The processed DataFrame is converted into Parquet format and uploaded to the designated AWS S3 data lake bucket with a `YYYY/MM/DD` partitioning scheme.

+--------------------------+     +-------------------------------+     +------------------+     +---------------------+     +--------------------------+
| AWS EventBridge          | --> | AWS Lambda Function           | --> | PurpleAir API    | --> | AWS Lambda Function | --> | AWS S3 Data Lake         |
| (Scheduled Trigger)      |     | (purpleair-ingest-function)   |     | (Data Fetch)     |     | (Processing)        |     | (Parquet, /YYYY/MM/DD/)  |
+--------------------------+     +-------------------------------+     +------------------+     +---------------------+     +--------------------------+

##  AWS Implementation Details

### 1. AWS S3 Data Lake Setup:

* **Bucket Creation:** An S3 bucket (`vinzenz-purpleair-data` or similar) was created to serve as the raw data storage layer.
* **Partitioning Strategy:** Data is organized into a `data/purpleair/YYYY/MM/DD/` folder structure, optimizing for time-series queries and cost efficiency with tools like AWS Athena.
* **File Format:** Data is stored in `.parquet` format for its columnar storage benefits.

### 2. AWS Lambda Function (`purpleair-ingest-function`):

* **Purpose:** The core compute engine for data ingestion and transformation.
* **Configuration:**
    * **Environment Variables:** Configured for sensitive API keys and sensor IDs.
    * **Memory & Timeout:** Adjusted to accommodate Python `pandas` and network operations.
* **Dependency Management (Lambda Layers):**
    * Utilized the AWS-provided Lambda Layer for `pandas`.
    * A custom Lambda Layer was created and deployed to include other necessary libraries like `requests` and `pyarrow`. This involved specific steps to ensure `pyarrow` binaries were compiled for the AWS Lambda Linux runtime environment (resolving `AttributeError: module 'os' has no attribute 'add_dll_directory'` and other compilation challenges), a key learning point.

### 3. AWS EventBridge Rule (Scheduler):

* A scheduled rule was configured to automatically trigger the `purpleair-ingest-function` every 6 hours, ensuring continuous data collection without manual intervention.


