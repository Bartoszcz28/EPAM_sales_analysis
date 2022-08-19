from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
import pandas as pd
import plotly
import plotly.express as px

master = "spark://spark:7077"
conf = SparkConf().setAppName("Task 6").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Reading, selecting columns and changing data types

orders = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_orders_dataset.csv") \
                            .select("order_id","order_delivered_carrier_date","order_estimated_delivery_date")
orders = orders.withColumn('order_delivered_carrier_date',to_date(unix_timestamp \
            (orders.order_delivered_carrier_date, 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))) \
            .withColumn('order_estimated_delivery_date',to_date(unix_timestamp \
            (orders.order_estimated_delivery_date, 'yyyy-MM-dd HH:mm:ss').cast('timestamp')))

# We drop rows with null values because we assume that these orders never will by deliverd
orders = orders.na.drop("any")

# two_days column description
  
# True - order was sent more then 2 days before expected delivery date   
# False - The order has not been shipped more than two days before the expected delivery date

orders_big = orders.withColumn('two_days', F.date_add(orders['order_delivered_carrier_date'],2))
orders_big = orders_big.withColumn("two_days", F.when(F.col("two_days") < F.col("order_estimated_delivery_date"), True) \
                                   .otherwise(False))

two_days = orders_big.groupBy("two_days").count()
two_days.write.parquet("/usr/local/spark/resources/data/transformed_data/6_task_two_days.parquet",mode="overwrite")