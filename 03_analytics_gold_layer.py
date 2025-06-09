# Databricks notebook source
from pyspark.sql.functions import month, year, sum, col

# COMMAND ----------

# Load Silver table
orders_df = spark.read.table("silver_customer_orders")

# COMMAND ----------

# Add year and month columns
orders_df = orders_df \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("revenue", col("quantity") * col("unit_price"))

# COMMAND ----------

# 1. Monthly demand per product per branch
monthly_demand = orders_df.groupBy("branch_id", "product_id", "year", "month") \
    .agg(sum("quantity").alias("total_quantity"))

monthly_demand.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("gold_monthly_product_demand")

# COMMAND ----------

# 2. Revenue trend per branch
branch_revenue = orders_df.groupBy("branch_id", "year", "month") \
    .agg(sum("revenue").alias("total_revenue"))

branch_revenue.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("gold_branch_revenue")

# COMMAND ----------

# 3. Top brands per month
top_brands = orders_df.groupBy("brand", "year", "month") \
    .agg(sum("quantity").alias("total_quantity")) \
    .orderBy(col("total_quantity").desc())

top_brands.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("gold_top_brands")

# COMMAND ----------

# 4. Optimize ZORDER for faster queries
spark.sql("OPTIMIZE gold_monthly_product_demand ZORDER BY (product_id)")
spark.sql("OPTIMIZE gold_branch_revenue ZORDER BY (branch_id)")
spark.sql("OPTIMIZE gold_top_brands ZORDER BY (brand)")