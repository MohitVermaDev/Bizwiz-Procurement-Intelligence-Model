# Databricks notebook source
from pyspark.sql.functions import to_date, col, lower, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# Load bronze data
bronze_orders = spark.read.format("delta").table("customer_orders")
bronze_stock = spark.read.format("delta").table("stock_levels")

# COMMAND ----------

# Transform and clean orders
orders_df = bronze_orders \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .filter(col("order_date").isNotNull())

# Filter invalid quantity and unit_price
orders_df = orders_df.filter((col("quantity") > 0) & (col("unit_price") > 0))

# Standardize product_name and brand to lowercase (simple enrichment)
orders_df = orders_df.withColumn("product_name", lower(col("product_name"))) \
                     .withColumn("brand", lower(col("brand")))

# Deduplicate by order_id, keeping latest order_date (in case of duplicates)
window_spec = Window.partitionBy("order_id").orderBy(col("order_date").desc())

orders_dedup = orders_df.withColumn("rn", row_number().over(window_spec)) \
                        .filter(col("rn") == 1) \
                        .drop("rn")

# Write silver customer orders partitioned by order_date and branch_id

orders_dedup.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date", "branch_id") \
    .saveAsTable('silver_customer_orders')

# COMMAND ----------

# Transform and clean stock levels
stock_df = bronze_stock.withColumn("last_updated", to_date(col("last_updated"), "yyyy-MM-dd")) \
    .filter(col("last_updated").isNotNull())

# Optional: filter out negative stock levels
stock_df = stock_df.filter(col("stock_level") >= 0)

# Write silver stock levels partitioned by branch_id
stock_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("branch_id") \
    .saveAsTable("silver_stock_levels")