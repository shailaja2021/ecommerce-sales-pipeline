from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "default"

BRONZE_CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"

SILVER_CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.silver_customers"
SILVER_PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.silver_products"
SILVER_ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"

# --- Silver: customers ---
df_customers = spark.table(BRONZE_CUSTOMERS_TABLE)

df_silver_customers = (
    df_customers
    .withColumn("region", F.upper(F.trim(F.col("region"))))
    .withColumn("email_missing", F.col("email").isNull())
)

print(f"customers in: {df_customers.count()}, customers out: {df_silver_customers.count()}")
df_silver_customers.groupBy("email_missing").count().show()

df_silver_customers.write.format("delta").mode("overwrite").saveAsTable(SILVER_CUSTOMERS_TABLE)
print(f"Wrote silver table: {SILVER_CUSTOMERS_TABLE}")

# --- Silver: products ---
df_products = spark.table(BRONZE_PRODUCTS_TABLE)

df_silver_products = df_products.withColumn("category", F.upper(F.trim(F.col("category"))))

print(f"products in: {df_products.count()}, products out: {df_silver_products.count()}")
df_silver_products.select("category").distinct().show()

df_silver_products.write.format("delta").mode("overwrite").saveAsTable(SILVER_PRODUCTS_TABLE)
print(f"Wrote silver table: {SILVER_PRODUCTS_TABLE}")

# --- Silver: orders ---
df_orders = spark.table(BRONZE_ORDERS_TABLE)
print(f"orders in (raw, with duplicates): {df_orders.count()}")

window_spec = Window.partitionBy("order_id").orderBy(F.lit(1))

df_orders_dedup = (
    df_orders
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"orders after dedup: {df_orders_dedup.count()}")

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "yyyy/MM/dd HH:mm"]

parsed_date_col = F.coalesce(
    *[F.try_to_timestamp(F.col("order_date"), F.lit(fmt)) for fmt in DATE_FORMATS]
)

df_orders_clean = df_orders_dedup.withColumn("order_date_parsed", parsed_date_col)

unparsed_count = df_orders_clean.filter(F.col("order_date_parsed").isNull()).count()
print(f"orders where order_date could not be parsed by any known format: {unparsed_count}")

df_orders_clean = df_orders_clean.withColumn("status", F.upper(F.trim(F.col("status"))))
df_orders_clean.select("status").distinct().show()

df_orders_clean = df_orders_clean.withColumn(
    "data_quality_flag",
    F.when(F.col("customer_id").isNull(), "missing_customer_id")
     .when(F.col("amount") <= 0, "invalid_amount")
     .otherwise("ok")
)

df_orders_clean.groupBy("data_quality_flag").count().show()

df_orders_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_ORDERS_TABLE)
print(f"Wrote silver table: {SILVER_ORDERS_TABLE}")