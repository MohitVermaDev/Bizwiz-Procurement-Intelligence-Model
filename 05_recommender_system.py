# Databricks notebook source
from pyspark.sql.functions import col, desc, row_number, collect_list
from pyspark.sql.window import Window

# COMMAND ----------

# Load cleaned customer order data
orders_df = spark.read.format("delta").table("silver_customer_orders")

# COMMAND ----------

# --- Recommender 1: Frequently Bought Together (pair frequency) ---

# Self-join to find product pairs in the same orders (basic co-occurrence)
product_pairs = orders_df.alias("a").join(
    orders_df.alias("b"),
    (col("a.order_id") == col("b.order_id")) & (col("a.product_id") < col("b.product_id"))
).select(
    col("a.product_id").alias("product_1"),
    col("b.product_id").alias("product_2"),
    col("a.branch_id")
)

frequent_pairs = product_pairs.groupBy("branch_id", "product_1", "product_2").count().orderBy(desc("count"))

# Save as Delta
frequent_pairs.write.format("delta").mode("overwrite").saveAsTable("recommendation_product_pairs")


# COMMAND ----------

# --- Recommender 2: Recent Top Products Per Customer (Top-N using window) ---

# Define window for latest orders per customer
window_spec = Window.partitionBy("customer_id").orderBy(desc("order_date"))

recent_orders = orders_df.withColumn("rank", row_number().over(window_spec)) \
                         .filter("rank <= 5") \
                         .select("customer_id", "product_id", "branch_id")

# Save for dashboarding or API-based recommendation
recent_orders.write.format("delta").mode("overwrite").saveAsTable("recent_top_products")


# COMMAND ----------

# --- Recommender 3: Branch-level Top Products to Recommend ---

branch_recos = orders_df.groupBy("branch_id", "product_id", "product_name") \
    .count().orderBy("branch_id", desc("count"))

branch_recos.write.format("delta").mode("overwrite").saveAsTable("branch_recommendations")

print("✅ Recommendation data saved to gold layer.")