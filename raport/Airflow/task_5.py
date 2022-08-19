from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
import plotly.express as px

master = "spark://spark:7077"
conf = SparkConf().setAppName("Task 5").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# read data
orders = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_orders_dataset.csv") \
                            .select("order_status")

delivery_pipeline = orders.groupby('order_status').count()
delivery_pipeline = delivery_pipeline.withColumnRenamed("count", "amount")
delivery_pipeline.write.parquet("/usr/local/spark/resources/data/transformed_data/5_task_delivery_pipeline.parquet" \
                                ,mode="overwrite")