# 🧠 Bizwiz: Procurement Intelligence - Lakehouse Project

This project showcases a **Procurement Intelligence** solution built on the **Lakehouse architecture** using **Databricks Community Edition**, **Delta Lake**, **PySpark**, **SQL**, and **Unity Catalog** concepts.

The system analyzes procurement and sales operations of **Kanini Haraka Enterprises Limited (KHEL)** to generate insights like demand forecasting, stock movement prediction, and procurement recommendations.

---

## 📂 Project Structure

```bash
bizwiz-procurement-intelligence/
│
├── 01_ingest_raw_data.py         # Ingest raw CSVs to Bronze layer with Delta Lake and task tracking
├── 02_transform_silver_layer.py  # Clean, validate and write Silver layer tables
├── 03_analytics_gold_layer.py    # Create aggregated Gold-level insights
├── 04_procurement_insights.py    # Demand trends and product-level intelligence
├── 05_recommender_system.py      # Recommender logic for what customers may buy next
├── README.md                     # Project documentation
├── /mnt/                         # Simulated lakehouse directory for Bronze/Silver/Gold
└── /Workspace/data/             # Source CSVs (e.g. `customer_orders.csv`, `stock_levels.csv`)

Copy the CSV from /Workspace/data/ and upload into DBFS and update the csv path in 01_ingest_raw_data.py

```

🏗️ Architecture: Lakehouse Pattern
Bronze Layer: Raw ingested data from CSV files

Silver Layer: Cleaned, validated, typed data

Gold Layer: Aggregated tables for analytics and reporting

All tables are Delta tables and optionally optimized using ZORDER for performance.

🛠️ Technologies Used
Tech	Purpose
Databricks (Community)	Development & orchestration
Delta Lake	ACID transactions + version control
PySpark	ETL & transformation logic
Unity Catalog (conceptual)	Data governance (simulated)
SQL / Spark SQL	Transformations and analytics
CSV	Source data format

🔁 ETL Flow
🔹 01 - Raw Ingestion
Reads customer_orders.csv and stock_levels.csv with schema

Uses Delta table task_tracker to enable incremental ingestion

Stores raw data in /mnt/bronze/

🔸 02 - Silver Transformation
Parses dates, standardizes schema

Writes cleaned tables to /mnt/silver/ as silver_customer_orders and silver_stock_levels

🟡 03 - Gold Analytics
Aggregates data for:

Monthly demand by product/branch

Branch revenue trends

Top-performing brands

Writes output to /mnt/gold/ and optimizes queries using ZORDER

📊 Key Insights Generated
Insight	Table Name
Monthly product demand	gold_monthly_product_demand
Branch-level revenue trends	gold_branch_revenue
Top brands per month	gold_top_brands
Recommender logic for products	On-demand (05_recommender_system.py)
Task Tracking (incremental ETL)	default.task_tracker

📌 Setup & Execution
✅ Prerequisites
Databricks Community Account

Upload source files:

/Workspace/data/customer_orders.csv

/Workspace/data/stock_levels.csv

▶️ Running the Pipeline
Run 01_ingest_raw_data.py

Loads CSVs to Bronze Delta tables

Uses task tracker to support incremental loading

Run 02_transform_silver_layer.py

Writes cleaned Silver tables

Run 03_analytics_gold_layer.py

Generates aggregated insights and stores in Gold tables

Explore insights

Use SQL or 04_procurement_insights.py and 05_recommender_system.py

🚀 Future Enhancements
Integrate with Power BI or Tableau for visualization

Add weather/school calendar dimension for demand simulation

Expand to HR, finance, and logistics modules

Add machine learning for stock prediction and dynamic pricing

👤 Author
Mohit Verma
Software Engineer | Data Engineer
GitHub: @mohitverma-code
