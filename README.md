# 🧩 Azure Data Engineering Project: End-to-End ETL Pipeline
This project demonstrates a complete ETL pipeline using Microsoft Azure services, based on the Medallion Architecture (Bronze → Silver → Gold). The goal was to implement data engineering best practices by implementing real-world data engineering concepts using the Azure ecosystem. The project ingests, transforms, and serves data to support reporting, ultimately showcasing the practical responsibilities of a Data Engineer in a cloud-based environment.

───────────────────────────────────────────────────────────────────────────────────────


# 📌 Project Objective

To implement a scalable Azure-based data pipeline that:

- Ingests structured data from GitHub
- Cleans and transforms it using Databricks (PySpark)
- Stores data in a structured format across Medallion Architecture layers
- Enables analytical access through Synapse Serverless SQL and visualization via Power BI

───────────────────────────────────────────────────────────────────────────────────────

# 🧬 Architecture Overview

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

# 🔧 Tools & Technologies

Tool/Service                                                        Role

Azure Data Factory

Move data from GitHub to Data Lake

Azure Data Lake Gen2

Store files across Bronze/Silver/Gold layers

Azure Databricks

Transform and join data using PySpark

PySpark

Perform distributed data processing

Azure Synapse Serverless SQL

Expose cleaned data for reporting

Power BI

Create simple visualizations to complete pipeline

  
