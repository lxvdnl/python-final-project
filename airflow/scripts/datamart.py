from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.functions import sum as spark_sum, when, expr

from pyspark.sql.functions import year, month, dayofmonth, split


def run_orders_datamart():


    JDBC_URL = "jdbc:postgresql://postgres:5432/project_db"
    JDBC_PROPS = {
        "user": "project_user",
        "password": "project_password",
        "driver": "org.postgresql.Driver"
    }



    spark = SparkSession.builder \
        .appName("datamart") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20") \
        .getOrCreate()


    orders = spark.read.jdbc(url=JDBC_URL, table="orders", 
                            column="order_id",
        lowerBound=1,
        upperBound=400000,
        numPartitions=10,
        properties=JDBC_PROPS)
    order_items = spark.read.jdbc(url=JDBC_URL, table="order_items",
                                column="order_id",
        lowerBound=1,
        upperBound=400000,
        numPartitions=10, properties=JDBC_PROPS)
    stores = spark.read.jdbc(url=JDBC_URL, table="stores", properties=JDBC_PROPS)
    delivery_assignments = spark.read.jdbc(url=JDBC_URL, table="delivery_assignments", properties=JDBC_PROPS)


    current_driver = (
        delivery_assignments
        .filter(col("unassigned_at").isNull())
        .groupBy("order_id")
        .agg(spark_max("driver_id").alias("driver_id"))
    )

    df = (
        orders
        .join(order_items, "order_id", "left")
        .join(current_driver, "order_id", "left")
        .join(stores, "store_id", "left")
    )


    df= df.repartition(10)


    df_metrics = (
        df
        .groupBy(
            "order_id",
            "user_id",
            "store_id",
            "status",
            "order_cancellation_reason",
            "created_at",
            "address_text",
            "driver_id",
            "delivered_at",
            "canceled_at"
        )
        .agg(
            spark_sum(
                col("item_quantity") * col("item_price") * (1 - col("item_discount")/100)
            ).alias("cash_flows"),

            spark_sum(
                when(col("status") == "canceled", 0)
                .otherwise(
                    (col("item_quantity") - col("item_canceled_quantity"))
                    * col("item_price")
                    * (1 - col("item_discount")/100)
                )
            ).alias("revenue")
        )
    )




    df_final = (
        df_metrics
        .withColumn(
            "is_cancel_error",
            when(
                (col("status") == "canceled") &
                (col("order_cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой")),
                1
            ).otherwise(0)
        )
        .withColumn(
            "is_cancel_after_delivery",
            when(
                col("delivered_at").isNotNull() &
                col("canceled_at").isNotNull() &
                (col("canceled_at") > col("delivered_at")),
                1
            ).otherwise(0)
        )
        .withColumn("city", split(col("address_text"), ",")[0])
        .withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
    )


    df_final = df_final.select(
        "order_id",
        "user_id",
        "store_id",
        "driver_id",
        "cash_flows",
        "revenue",
        "status",
        "order_cancellation_reason",
        "is_cancel_error",
        "is_cancel_after_delivery",
        "city",
        "year",
        "month",
        "day"
    )


    df_final = df_final.repartition(10)


    df_final.write.jdbc(
        url=JDBC_URL,
        table="orders_datamart",
        mode="overwrite", 
        properties=JDBC_PROPS
    )
pass




