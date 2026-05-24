from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import expr

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

orders_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)



orders_json = orders_raw.selectExpr(
    "CAST(value AS STRING)"
)



order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_time", StringType())
])



orders = (
    orders_json
    .select(
        from_json(
            col("value"),
            order_schema
        ).alias("data")
    )
    .select("data.*")
)

customers = spark.read.csv(
    "data/customers.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    "data/products.csv",
    header=True,
    inferSchema=True
)

customers = customers.select(
    "customer_id",
    "name",
    "city"
)

products = products.select(
    "product_id",
    "product_name",
    "category",
    "price"
)

orders_customers = orders.join(
    customers,
    on="customer_id",
    how="left"
)

enriched_orders = orders_customers.join(
    products,
    on="product_id",
    how="left"
)


enriched_orders = enriched_orders.withColumn(
    "revenue",
    expr("quantity * price")
)

enriched_orders = enriched_orders.select(
    "order_id",
    "customer_id",
    "name",
    "city",
    "product_name",
    "category",
    "quantity",
    "price",
    "revenue",
    "order_time"
)


def write_to_mysql(batch_df, batch_id):

    print(f"\n===== Batch {batch_id} =====")

    batch_df.show(truncate=False)

    (
        batch_df.write
        .format("jdbc")
        .option(
        "url",
        "jdbc:mysql://127.0.0.1:3307/ecommerce_db"
        )

        .option(
            "dbtable",
            "sales_fact"
        )
        .option(
            "user",
            "root"
        )
        .option(
            "password",
            "root"
        )
        .option(
            "driver",
            "com.mysql.cj.jdbc.Driver"
        )
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written successfully")


query = (
    enriched_orders.writeStream
    .foreachBatch(write_to_mysql)
    .outputMode("append")
    .start()
)

query.awaitTermination()