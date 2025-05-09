# 🧩 Azure Data Engineering Project: End-to-End ETL Pipeline
This project demonstrates a complete ETL pipeline using Microsoft Azure services, based on the Medallion Architecture (Bronze → Silver → Gold). The goal was to implement data engineering best practices by implementing real-world data engineering concepts using the Azure ecosystem. The project ingests, transforms, and serves data to support reporting, ultimately showcasing the practical responsibilities of a Data Engineer in a cloud-based environment.

───────────────────────────────────────────────────────────────────────────────────────


## 🎯 Project Objective

To implement a scalable Azure-based data pipeline that:

- Ingests structured data from GitHub
- Cleans and transforms it using Databricks (PySpark)
- Stores data in a structured format across Medallion Architecture layers
- Enables analytical access through Synapse Serverless SQL and visualization via Power BI

───────────────────────────────────────────────────────────────────────────────────────

## 🧬 Architecture Overview

<pre> [GitHub CSV Files]
      |
      v
Azure Data Factory (Ingestion Pipeline)
      |
      v
Azure Data Lake Storage Gen2
├── Bronze: Raw data (as-is)
├── Silver: Cleaned & joined data using Databricks
└── Gold: Final analytical tables
      |
      v
Azure Synapse Analytics (Serverless SQL Pool)
      |
      v
Power BI Dashboard (Simple reporting layer) </pre>

───────────────────────────────────────────────────────────────────────────────────────

## 🛠️ Tools & Technologies

| 🧰 Tool/Service              | 🔍 Purpose                                                  |
|-----------------------------|-------------------------------------------------------------|
| Azure Data Factory          | Data ingestion from GitHub to DataLake (CSV to ADLS Gen2 - Bronze)     |
| Azure Data Lake Gen2        | Structured storage for Bronze, Silver, and Gold layers      |
| Azure Databricks            | Data cleaning and transformation using PySpark              |
| PySpark                     | Distributed data processing in Databricks                   |
| Azure Synapse Serverless SQL| Expose external tables and views for querying               |
| Power BI                    | Data visualization and reporting from Synapse               |

───────────────────────────────────────────────────────────────────────────────────────
## 🗃️ Project Breakdown

### 1️⃣ Data Ingestion (Bronze Layer)

- Used Azure Data Factory to copy CSVs from GitHub into a raw container in ADLS

- Stored each file in its original format in a structured folder without modification.

### 2️⃣ Data Transformation (Silver Layer)

- Established secure connection between Azure Data Lake and Databricks

- Loaded multiple CSVs (Product, Customer, Sales, Returns) into Databricks

- Used PySpark to clean, filter, and transform the raw data

- Performed table joins to generate enriched datasets for analysis

- Stored the output data into a Silver container in cleaned format

### 3️⃣ Data Serving (Gold Layer)

- Created external tables in Synapse Serverless SQL using OPENROWSET

- Loaded transformed data into these external tables for optimized querying and reporting access

### 4️⃣ Reporting (Power BI)

- Connected Power BI to Synapse Serverless SQL using DirectQuery

- Developed an interactive dashboard that includes:

- Total Customers (Card visualization)

- Order Quantity Trend (Line Chart)

- Revenue by Product Category (Bar and Pie Chart using calculated measure)

- Top Performing Products (Horizontal Bar Chart)

───────────────────────────────────────────────────────────────────────────────────────

## 🎓 Key Takeaways

- Implemented Medallion Architecture in a real-world Azure data pipeline

- Established secure Azure service connections (Data Lake ↔ Databricks ↔ Synapse)

- Applied PySpark for real-time data cleaning and transformation

- Queried transformed data through Synapse external tables

- Integrated Synapse with Power BI for business-ready reporting.

───────────────────────────────────────────────────────────────────────────────────────

## 📷 Project Visual Demo


  
