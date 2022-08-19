from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
import plotly.express as px

master = "spark://spark:7077"
conf = SparkConf().setAppName("Task 2").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Reading csv file
products = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_products_dataset.csv") \
                            .select("product_id", "product_category_name")
order_items = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                            .csv("/usr/local/spark/resources/data/olist_order_items_dataset.csv") \
                            .select(col("product_id").alias("product_id_2"))
product_category = spark.read.options(header="True", inferSchema="True", delimiter=",") \
                            .csv("/usr/local/spark/resources/data/product_category_name_translation.csv")

products = products.join(product_category,
              products.product_category_name == product_category.product_category_name,
              "Inner").select("product_id", "product_category_name_english")\
            .withColumnRenamed("product_category_name_english", "product_category_name")


join_table = order_items.join(products, order_items.product_id_2==products.product_id, "Inner")
join_table = join_table.select(join_table.columns[-2:])

all_products_count = join_table.count()

join_table.createOrReplaceTempView("join_table")

top_10_categories = spark.sql("""
    SELECT product_category_name FROM
            (SELECT COUNT(*), product_category_name FROM join_table
            GROUP BY product_category_name
            ORDER BY 1 DESC
            LIMIT 10)
""")

top_10_categories.createOrReplaceTempView("top_10_categories")

orders_from_top_10_categories = spark.sql("""
    SELECT * FROM join_table
    WHERE product_category_name IN
        -- top 10 best selling categories
        (SELECT * FROM top_10_categories)
    ;
""")

orders_from_top_10_categories.createOrReplaceTempView("orders_from_top_10_categories")

top_10_categories_count = spark.sql("""
    SELECT * from orders_from_top_10_categories;
""").count()


top_10_categories_counts_df = spark.sql(f"""
    SELECT COUNT(*) AS count, product_category_name from orders_from_top_10_categories
        GROUP BY product_category_name
        ORDER BY count DESC
    ;
""")

top_10_categories_counts_df = top_10_categories_counts_df\
    .union(spark.createDataFrame(Row([all_products_count - top_10_categories_count, "rest"]),
                                 ["count", "product_category_name"]))


top_10_categories_counts_df.write.parquet("/usr/local/spark/resources/data/transformed_data/2_task_top_10_categories.parquet" \
                           ,mode="overwrite")


top_10_categories_counts_df.show()


# Task 2 solution

top_3_products = spark.sql("""WITH temp AS (
               SELECT
               product_category_name,
               product_id,
               COUNT(product_id) AS together
               FROM join_table
               GROUP BY product_category_name, product_id
               )
               SELECT product_category_name, product_id, together
               FROM (SELECT *,
               ROW_NUMBER() OVER(PARTITION BY product_category_name ORDER BY together DESC) AS row_number
               FROM temp)
               WHERE row_number < 4
               """)

top_3_products.write.parquet("/usr/local/spark/resources/data/transformed_data/2_task_top_3_products.parquet" \
                           ,mode="overwrite")

top_3_products.createOrReplaceTempView("top3products")
top_3_from_top_10_categories = spark.sql("""
    SELECT *,
    CAST(ROW_NUMBER() OVER (PARTITION BY product_category_name ORDER BY together DESC) AS string) as product_number
    FROM top3products
    WHERE product_category_name IN
        (SELECT product_category_name FROM top_10_categories)
    ;
""")
top_3_from_top_10_categories.write.parquet("/usr/local/spark/resources/data/transformed_data/2_task_top_3_products_from_top_10_categories.parquet" \
                           ,mode="overwrite")