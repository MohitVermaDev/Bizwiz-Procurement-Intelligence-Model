# Databricks notebook source
from pyspark.sql.functions import sum, desc, countDistinct, month, year, to_date

# COMMAND ----------

# ------------------------------------------
# 🎛️ Widgets for dynamic date range
# ------------------------------------------
dbutils.widgets.text("FROM_DATE", "2024-01-01")
dbutils.widgets.text("TO_DATE", "2024-12-31")

FROM_DATE = dbutils.widgets.get("FROM_DATE")
TO_DATE = dbutils.widgets.get("TO_DATE")

print(f"📅 Analyzing orders from {FROM_DATE} to {TO_DATE}")

# COMMAND ----------

# ------------------------------------------
# 🔄 Load Silver Orders Layer
# ------------------------------------------
orders_df = spark.read.format("delta").table("silver_customer_orders") \
    .withColumn("order_date", to_date("order_date"))

# Filter orders by date range
orders_df = orders_df.filter((orders_df.order_date >= FROM_DATE) & (orders_df.order_date <= TO_DATE))


# COMMAND ----------

# ------------------------------------------
# 🔍 Insight 1: Top Selling Products
# ------------------------------------------
top_products = orders_df.groupBy("product_id", "product_name") \
    .agg(sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity"))

top_products.write.format("delta").mode("overwrite").saveAsTable("top_selling_products")


# COMMAND ----------

# ------------------------------------------
# 🔍 Insight 2: Revenue per Branch
# ------------------------------------------
branch_revenue = orders_df.withColumn("revenue", orders_df.quantity * orders_df.unit_price) \
    .groupBy("branch_id", "branch_name") \
    .agg(sum("revenue").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

branch_revenue.write.format("delta").mode("overwrite").saveAsTable("branch_revenue")


# COMMAND ----------

# ------------------------------------------
# 🔍 Insight 3: Brand Performance
# ------------------------------------------
brand_perf = orders_df.groupBy("brand") \
    .agg(sum("quantity").alias("total_units_sold")) \
    .orderBy(desc("total_units_sold"))

brand_perf.write.format("delta").mode("overwrite").saveAsTable("top_brands")

# COMMAND ----------

# ------------------------------------------
# 🔍 Insight 4: SKU-wise Branch Performance
# ------------------------------------------
sku_branch_perf = orders_df.groupBy("branch_id", "sku") \
    .agg(sum("quantity").alias("units_sold")) \
    .orderBy("branch_id", desc("units_sold"))

sku_branch_perf.write.format("delta").mode("overwrite").saveAsTable("sku_performance")


# COMMAND ----------

# ------------------------------------------
# 🔍 Insight 5: Monthly Product Trend
# ------------------------------------------
monthly_trend = orders_df.groupBy("product_id", "product_name", month("order_date").alias("month")) \
    .agg(sum("quantity").alias("monthly_quantity")) \
    .orderBy("product_id", "month")

monthly_trend.write.format("delta").mode("overwrite").saveAsTable("monthly_product_trend")

print("✅ Procurement insights generated successfully with widget-based date filters.")