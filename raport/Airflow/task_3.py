from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as f
from pyspark.sql.window import Window

master = "spark://spark:7077"
conf = SparkConf().setAppName("Task 3").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#  Reading csv file
# 4
olist_order_payments_dataset = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_order_payments_dataset.csv")\
                            .select("order_id", "payment_value")
# 6
olist_orders_dataset = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_orders_dataset.csv")\
                            .select(f.col("order_id").alias("order_id_orders"), "customer_id", "order_purchase_timestamp")\
                            .filter(f.col("order_status") != "canceled")


olist_orders_dataset = olist_orders_dataset.withColumn('order_purchase_year', f.year(f.to_date('order_purchase_timestamp')))\
                                           .withColumn('order_purchase_month', f.month(f.to_date('order_purchase_timestamp')))\
                                           .drop('order_purchase_timestamp')
 
# Taking data from 2017 because the oldest data comes from 2018
olist_orders_dataset = olist_orders_dataset.where(olist_orders_dataset.order_purchase_year == '2017')\
                                           .drop('order_purchase_year')
                                                       

# Joining olist_order_payments_dataset and olist_orders_dataset
join_table = olist_orders_dataset.join(olist_order_payments_dataset, olist_orders_dataset.order_id_orders == olist_order_payments_dataset.order_id, 'inner')\
                                 .drop('order_id_orders')


# Taking maximum spending customers for every month
join_table_grouped = (join_table.groupBy('order_purchase_month', 'customer_id').sum('payment_value')).withColumnRenamed("sum(payment_value)", "month_payment")


window = Window.partitionBy('order_purchase_month').orderBy(f.col('month_payment').desc())
result_table = join_table_grouped.withColumn('row', f.row_number().over(window))\
                                 .filter(f.col('row') == 1).drop('row').orderBy('order_purchase_month')

result_table.write.parquet("/usr/local/spark/resources/data/transformed_data/3_task_month_spending.parquet",mode="overwrite")