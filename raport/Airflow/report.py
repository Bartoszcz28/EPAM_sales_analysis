from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
# from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
import pandas as pd
import plotly
import plotly.express as px
import folium
from folium import plugins
#import codecs

master = "spark://spark:7077"
conf = SparkConf().setAppName("Report").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# read data
# task 2 data
top_10_categories = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/2_task_top_10_categories.parquet")
top_3_products = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/2_task_top_3_products.parquet")
# task 3 data
month_spending = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/3_task_month_spending.parquet")
# task 4 data
salescity_df = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/4_task_salescity_df.parquet")
salesstate_df = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/4_task_salesstate_df.parquet")
city_best_sellers_df = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/4_task_city_best_sellers_df.parquet")
# task 5 data
task_5_delivery_pipeline = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/5_task_delivery_pipeline.parquet")
# task 6 data
task_6_two_days = spark.read.parquet("/usr/local/spark/resources/data/transformed_data/6_task_two_days.parquet")

# Task 2
top_10_categories_pd = top_10_categories.toPandas()
task_2_fig = px.pie(top_10_categories_pd, values="count", names="product_category_name")

# Task 3
month_spending = month_spending.toPandas()
task_3_fig = px.bar(month_spending, x='order_purchase_month', y='month_payment', color='customer_id', color_discrete_sequence=px.colors.qualitative.Dark24,
             title='Maximum spending customers for every month',
             labels={
                 "month_payment":"Amount spent",
                 "customer_id": "Customer ID",
                 "order_purchase_month": 'Month'
             })

task_3_fig.update_layout(
    xaxis = dict(
        tickmode = 'array',
        tickvals = [i for i in range(1, 13)],
        ticktext = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    )
)

# Task 6
task_6_two_days = task_6_two_days.toPandas()
task_6_fig = px.pie(task_6_two_days, values='count', 
             names=["Shipped more than 2 days before scheduled delivery date","Shipped less than 2 days before scheduled delivery date"],
             title='Percentage of parcels not shipped more than 2 days before the scheduled delivery date')

# Task 5 
pd_delivery_pipeline = task_5_delivery_pipeline.toPandas()
pd_delivery_pipeline.head()
pie_chart = pd_delivery_pipeline.drop(pd_delivery_pipeline[pd_delivery_pipeline.order_status ==  "delivered"].index)
task_5_fig = px.pie(pie_chart, values=pie_chart["amount"], 
             names=pie_chart["order_status"],
             title='Order Status')

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
df2 = rdd.toDF(["seller_state","revenue","count","lat", "lng", "size", "colors"])
rdd_2 = salescity_df.rdd.map(lambda row: (
      row["seller_city"],row["revenue"],row["count"],row["lat"],row["lng"], sizes[count_group(row["count"])], colors[count_group(row["count"])])
  )
df3 = rdd_2.toDF(["seller_city","revenue","count","lat", "lng", "size", "colors"])
city_best_sellers_df = city_best_sellers_df.withColumnRenamed('seller_city', 'seller_city_2')
city_data_joined = df3.join(city_best_sellers_df, df3.seller_city==city_best_sellers_df.seller_city_2, 'left').drop('seller_city_2')
cities = city_data_joined.toPandas()
task_4_map = px.scatter_mapbox(cities, lat="lat", lon="lng",
                         size="size",
                         hover_data={'lat':False, 'lng':False, 'size':False, '1_best_seller':True, '2_best_seller':True},
                         hover_name="seller_city",
                         color_discrete_sequence=[cities.colors], 
                         labels={
                                 "1_best_seller":"1 best seller",
                                 "2_best_seller":"2 best seller"
                             },
                         zoom=3, height=300)
task_4_map.update_layout(mapbox_style="open-street-map")
task_4_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

with open('/usr/local/spark/resources/data/report.html', 'w') as f:
    f.write('<h2>Percentage of parcels not shipped more than 2 days before the scheduled delivery date</h2>')
    f.write(task_6_fig.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write('<h2>Order Status</h2>')
    f.write(task_5_fig.to_html(full_html=False, include_plotlyjs='cdn'))
    
    f.write(task_4_map.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(task_3_fig.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(task_2_fig.to_html(full_html=False, include_plotlyjs='cdn'))