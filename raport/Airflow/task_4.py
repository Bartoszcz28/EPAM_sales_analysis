import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import folium
from folium import plugins

##########################
# You can configure master here if you do not pass the spark.master paramenter in conf
##########################
master = "spark://spark:7077"
conf = SparkConf().setAppName("Task 4").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Reading data from parquet and selecting only needed columns
# Orders data
items = spark.read.csv('/usr/local/spark/resources/data/olist_order_items_dataset.csv',header=True,inferSchema=True)\
    .select('order_id','seller_id','price')
# Sellers data 
sellers = spark.read.csv('/usr/local/spark/resources/data/olist_sellers_dataset.csv',header=True,inferSchema=True)\
    .select('seller_id','seller_zip_code_prefix','seller_city','seller_state')\
    .withColumnRenamed('seller_zip_code_prefix','zip_code')
# Geolocation data 
geo = spark.read.csv('/usr/local/spark/resources/data/olist_geolocation_dataset.csv',header=True,inferSchema=True)\
    .select('geolocation_zip_code_prefix','geolocation_lat','geolocation_lng')\
    .withColumnRenamed('geolocation_zip_code_prefix','zip_code')\
    .groupBy('zip_code')\
    .agg({'geolocation_lat':'avg','geolocation_lng':'avg'})\
    .withColumnRenamed('avg(geolocation_lat)','lat')\
    .withColumnRenamed('avg(geolocation_lng)','lng')

# Orders data 
orders = spark.read.csv('/usr/local/spark/resources/data/olist_orders_dataset.csv',header=True,inferSchema=True)\
    .select('order_id','order_purchase_timestamp')

# |-- order_id: string (nullable = true)
# |-- seller_id: string (nullable = true)
# |-- price: double (nullable = true)

# |-- seller_id: string (nullable = true)
# |-- seller_zip_code_prefix: integer (nullable = true)
# |-- seller_city: string (nullable = true)
# |-- seller_state: string (nullable = true)

# |-- geolocation_zip_code_prefix: integer (nullable = true)
# |-- lat: double (nullable = true)
# |-- lng: double (nullable = true)

# |-- order_id: string (nullable = true)
# |-- order_purchase_timestamp: string (nullable = true)/home/jovyan/work/sales_analysis/



# Calculating sum of money earned by sellers 
# and joining sellers table to be able to calculate 
# money earned partitioning by location 
sales = items\
    .groupBy('seller_id')\
    .agg({'price':'sum'})\
    .withColumnRenamed('sum(price)','revenue')\
    .join(sellers,['seller_id'])

# Top 2 sellers in every state
#using pyspark dataframe

# Partition by state
window_state = Window.partitionBy('seller_state').orderBy(col('revenue').desc())
salesstate_df = sales\
    .withColumn('rank',rank().over(window_state))\
    .filter(col('rank')<=2)\
    .select('seller_id','seller_state','revenue')\
    .orderBy(col('revenue').desc())

# Top 2 sellers in every city
# using pyspark dataframe

# Partition by city
window_state = Window.partitionBy('seller_city').orderBy(col('revenue').desc())
salescity_df = sales\
    .withColumn('rank',rank().over(window_state))\
    .filter(col('rank')<=2)\
    .select('seller_id','seller_city','revenue')\
    .orderBy(col('revenue').desc())

# Creating DataFrame containing seller_city, 1_best_seller, 2_best_seller
window_seller = Window.partitionBy('seller_city').orderBy(col('revenue').desc())
city_sellers = salescity_df.withColumn('rank',rank().over(window_seller))
best_sellers = city_sellers.filter(city_sellers.rank <= 2)\
   .withColumn('col', expr('concat(rank, "_best_seller")'))\
   .groupby('seller_city')\
   .pivot('col')\
   .agg(first(city_sellers.seller_id))\
   .na.fill("In this city is only one seller", ["2_best_seller"])

best_sellers.write.parquet("/usr/local/spark/resources/data/transformed_data/4_task_city_best_sellers_df.parquet",mode="overwrite")

# Creating data frame with different locations sum of sales and number of sales 
salesmap_df = sales.join(geo,'zip_code','left').orderBy(col('revenue').desc())
salescity_df = salesmap_df\
    .groupby('seller_city')\
    .agg({'revenue':'sum','lat':'avg','lng':'avg','seller_state':'count'})\
    .orderBy(col('sum(revenue)'))\
    .select('seller_city',
            col('sum(revenue)').alias('revenue'),
            col('count(seller_state)').alias('count'),
            col('avg(lat)').alias('lat'),
            col('avg(lng)').alias('lng'))

salesstate_df = salesmap_df\
    .groupby('seller_state')\
    .agg({'revenue':'sum','lat':'avg','lng':'avg','seller_city':'count'})\
    .orderBy(col('sum(revenue)'))\
    .select('seller_state',
            col('sum(revenue)').alias('revenue'),
            col('count(seller_city)').alias('count'),
            col('avg(lat)').alias('lat'),
            col('avg(lng)').alias('lng'))

# Function for further grouping of our results 
def count_group(x):
    if x>1500:
        return 0
    elif x>200:
        return 1
    elif x>100:
        return 2
    elif x>20:
        return 3
    elif x>10:
        return 4
    else:
        return 5
def sum_group(x):
    if x>8000000:
        return 0
    elif x>1000000:
        return 1
    elif x>500000:
        return 2
    elif x>50000:
        return 3
    elif x>10000:
        return 4
    else:
        return 5
    
colors = ['#CE4C18','#D58321','#D5B421','#C3E839','#43A85F','#66CF83']
sizes = [25,12,8,5,3,1]

rdd = salesstate_df.rdd.map(lambda row: (
      row["seller_state"],row["revenue"],row["count"],row["lat"],row["lng"], sizes[count_group(row["count"])], colors[count_group(row["count"])])
  )
state_df = rdd.toDF(["seller_state","revenue","count","lat", "lng", "size", "colors"])

rdd_2 = salescity_df.rdd.map(lambda row: (
      row["seller_city"],row["revenue"],row["count"],row["lat"],row["lng"], sizes[count_group(row["count"])], colors[count_group(row["count"])])
  )
city_df = rdd_2.toDF(["seller_city","revenue","count","lat", "lng", "size", "colors"])

state_df.write.parquet("/usr/local/spark/resources/data/transformed_data/4_task_salesstate_df.parquet" \
                            ,mode="overwrite")
city_df.write.parquet("/usr/local/spark/resources/data/transformed_data/4_task_salescity_df.parquet" \
                           ,mode="overwrite")