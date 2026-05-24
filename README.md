🚀 Real-Time Ecommerce Analytics Pipeline

A real-time data engineering project that simulates e-commerce order events, processes them using Apache Spark Structured Streaming, stores enriched data in MySQL, and visualizes business insights through Grafana dashboards.

📌 Project Overview

This project demonstrates an end-to-end streaming data pipeline commonly used in modern data engineering systems.

Orders are continuously generated and published to Kafka. PySpark consumes these events in real time, enriches them with customer and product information, calculates revenue metrics, and stores the processed data in MySQL. Grafana is then used to build interactive dashboards for monitoring sales performance.

🏗️ Architecture

┌─────────────────┐

│ Order Producer  │

│ (Python)        │

└────────┬────────┘

         │
         
         ▼
┌─────────────────┐

│ Apache Kafka    │

│ orders topic    │

└────────┬────────┘

         │
         
         ▼

┌───────────────────────────┐

│ PySpark Structured Stream │

│                           │

│ • Consume Kafka events    │

│ • Parse JSON orders       │

│ • Join Customer Data      │

│ • Join Product Data       │

│ • Calculate Revenue       │

└────────┬──────────────────┘

         │
         ▼

┌─────────────────┐

│ MySQL           │

│ sales_fact      │

└────────┬────────┘

         │
         
         ▼

┌─────────────────┐

│ Grafana         │


│ Dashboards      │

└─────────────────┘

🛠️ Tech Stack
Technology	Purpose
Python	Order Producer
Apache Kafka	Real-time Event Streaming
PySpark	Stream Processing
Structured Streaming	Real-time ETL
MySQL	Data Storage
Grafana	Visualization
Docker Compose	Container Orchestration
📂 Project Structure

ecommerce-analytics/

│

├── data/

│   ├── customers.csv

│   └── products.csv

│

├── producer/
│   └── order_producer.py

│

├── spark/

│   └── stream_processor.py

│

├── dashboards/

│   └── grafana_dashboard.json

│

├── docker-compose.yml

│

└── README.md

📊 Dataset
Customers
customer_id,name,city,age
1,Alice,Mumbai,31
2,Bob,Delhi,28
...
Products
product_id,product_name,category,price
101,Laptop,Electronics,70000
102,Phone,Electronics,35000
...
⚙️ Features
Real-Time Order Generation

Simulates customer purchases continuously.

Example event:

{
  "order_id": 21917,
  "customer_id": 7,
  "product_id": 102,
  "quantity": 2,
  "order_time": "2026-05-24T11:47:18"
}
Kafka Streaming

Orders are pushed into the Kafka topic:

orders
Spark Streaming ETL

PySpark performs:

Kafka ingestion
JSON parsing
Customer enrichment
Product enrichment
Revenue calculation

Revenue formula:

Revenue = Quantity × Price
Data Warehouse Table

Processed records are stored in:

sales_fact

Schema:

CREATE TABLE sales_fact (
    order_id BIGINT,
    customer_id INT,
    name VARCHAR(100),
    city VARCHAR(100),
    product_name VARCHAR(100),
    category VARCHAR(100),
    quantity INT,
    price DOUBLE,
    revenue DOUBLE,
    order_time VARCHAR(100)
);
📈 Grafana Dashboard

The dashboard provides real-time business insights:

Top Products

Displays products generating the highest revenue.

Total Orders

Shows total processed orders.

Revenue by Category

Breakdown of revenue across categories.

Total Revenue

Displays cumulative revenue generated.

Top Customers

Identifies highest spending customers.

📷 Dashboard Preview

<img width="1514" height="958" alt="image" src="https://github.com/user-attachments/assets/37b5c8fe-f4bb-467c-9324-5217c79fbdab" />


🚀 Running the Project
Start Infrastructure
docker compose up -d
Start Kafka Producer
python producer/order_producer.py
Start Spark Streaming Job
spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.mysql:mysql-connector-j:8.3.0 \
spark/stream_processor.py
Verify Data in MySQL
USE ecommerce_db;

SELECT COUNT(*) FROM sales_fact;
Open Grafana
http://localhost:3000

Default Credentials:

Username: admin
Password: admin
📊 Sample Results
Product	Revenue
Laptop	4,565,000
Phone	2,525,000
Watch	475,000
Shoes	303,000
Book	49,700
🎯 Key Learnings
Kafka Producer/Consumer Architecture
PySpark Structured Streaming
Stream ETL Processing
Data Enrichment using Joins
JDBC Integration with MySQL
Docker-based Infrastructure
Real-Time Dashboarding using Grafana
🔮 Future Enhancements
Airflow Workflow Orchestration
Data Quality Checks
Incremental Aggregations
AWS Deployment (EC2 + RDS + MSK)
Spark Checkpointing
Real-Time Alerting in Grafana
Data Lake Storage (S3/MinIO)
👨‍💻 Author

Viveena Khatri

Data Engineering | Python | PySpark | Kafka | SQL | Grafana | Docker
