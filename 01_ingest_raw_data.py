# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, to_date, lit, current_timestamp, max as spark_max, to_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

stock_level_path = "/FileStore/tables/stock_levels.csv"
customer_orders_path = "/FileStore/tables/customer_orders.csv"

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("branch_name", StringType(), True)
])

# Create schema for stock levels
stock_schema = StructType([
    StructField("branch_id", IntegerType(), True),
    StructField("branch_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("stock_level", IntegerType(), True),
    StructField("last_updated", StringType(), True)
])

# COMMAND ----------

# Create task_tracker table if not exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.task_tracker (
        task_id STRING,
        task_name STRING,
        status STRING,
        last_updated TIMESTAMP
    )
    USING DELTA
    LOCATION '/mnt/meta/task_tracker'
""")

# Get last ingested order_date for incremental load
last_ingested_date_df = spark.sql("""
    SELECT MAX(last_updated) as max_date
    FROM default.task_tracker
    WHERE task_id = 'customer_orders_load'
""")
last_ingested_date = last_ingested_date_df.collect()[0]["max_date"] if last_ingested_date_df.collect()[0]["max_date"] else "2000-01-01"

# COMMAND ----------

spark.read.csv(stock_level_path, schema=stock_schema, header=True).write.mode('overwrite').option("mergeSchema", "true").saveAsTable('stock_levels')

# COMMAND ----------

print(last_ingested_date)

# COMMAND ----------

# Read new incoming orders
orders_df = spark.read.csv(customer_orders_path, header=True, schema=orders_schema)
orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Filter records after last ingested date
incremental_orders_df = orders_df.filter(col("order_date") > lit(last_ingested_date))

# Perform merge if table exists
if spark._jsparkSession.catalog().tableExists("default.customer_orders"):
    delta_table = DeltaTable.forName(spark, "default.customer_orders")

    delta_table.alias("target").merge(
        incremental_orders_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print('Delta Execution')
else:
    # If table doesn't exist, write it
    incremental_orders_df.write.format("delta").saveAsTable("customer_orders")
    print('Simple save')
spark.sql("OPTIMIZE default.customer_orders ZORDER BY (order_date)")

# COMMAND ----------

# Update task tracker
if incremental_orders_df.count() > 0:
    latest_date = incremental_orders_df.agg(spark_max("order_date")).collect()[0][0]
    tracker_df = spark.createDataFrame([
        ("customer_orders_load", "Ingest customer orders", "SUCCESS", str(latest_date))
    ], ["task_id", "task_name", "status", "last_updated"]) \
    .withColumn("last_updated", to_timestamp("last_updated"))
    tracker_df.write.format("delta").mode("append").saveAsTable("default.task_tracker")