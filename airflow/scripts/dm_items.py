# dm_items.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, when, split, date_format


def run_item_datamart():
    JDBC_URL = "jdbc:postgresql://postgres:5432/project_db"
    JDBC_PROPS = {
        "user": "project_user",
        "password": "project_password",
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .appName("items_datamart") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20") \
        .getOrCreate()


    orders = spark.read.jdbc(url=JDBC_URL, table="orders", properties=JDBC_PROPS)
    order_items = spark.read.jdbc(url=JDBC_URL, table="order_items", properties=JDBC_PROPS)
    items = spark.read.jdbc(url=JDBC_URL, table="items", properties=JDBC_PROPS)
    stores = spark.read.jdbc(url=JDBC_URL, table="stores", properties=JDBC_PROPS)


    df = orders \
        .join(order_items, "order_id", "left") \
        .join(items, "item_id", "left") \
        .join(stores, "store_id", "left")


    df = df.withColumn("year", date_format(col("created_at"), "yyyy").cast("int")) \
        .withColumn("month", date_format(col("created_at"), "MM").cast("int")) \
        .withColumn("day", date_format(col("created_at"), "dd").cast("int")) \
        .withColumn("city", split(col("address_text"), ",")[0])


    df_items = df.groupBy(
        "year",
        "month",
        "day",
        "city",
        "store_id",
        "item_category",
        "item_id",
        "item_title"
    ).agg(
        spark_sum(col("item_quantity") * col("item_price") * (1 - col("item_discount")/100)).alias("item_cash_flows"),
        spark_sum(col("item_quantity")).alias("ordered_qty"),
        spark_sum(col("item_canceled_quantity")).alias("canceled_qty"),
        countDistinct(col("order_id")).alias("orders_cnt"),
        countDistinct(when(col("item_canceled_quantity") > 0, col("order_id"))).alias("orders_with_canceled_items")
    )

    df_items = df_items.select(
            "store_id" ,
            "item_category" ,
            "item_id" ,
            "item_title" ,
            "item_cash_flows" ,
            "ordered_qty" ,
            "canceled_qty" ,
            "orders_cnt" ,
            "orders_with_canceled_items" ,
            "year" ,
            "month" ,
            "day" ,
            "city"
        )



    df_items = df_items.repartition(10)


    df_items.write.jdbc(
        url=JDBC_URL,
        table="items_datamart",
        mode="overwrite",
        properties=JDBC_PROPS
    )
pass