from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import os

os.environ['HADOOP_HOME'] = "C:\\hadoop-3.3.0"

# Create a SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .enableHiveSupport() \
    .config('spark.jars.packages',
            'net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3') \
    .getOrCreate()

# Define file paths
data_dir = "D:\Gurudev\products_json"
CUSTOMERS = f"{data_dir}/customer_updated.json"
SALES = f"{data_dir}/sales.json"


customer_df = spark.read.option("multiline", "true").json(CUSTOMERS)
sales_df = spark.read.option("multiline", "true").json(SALES)

##################### CURATED LAYER ################################

sales_datatype_df = sales_df.withColumn('OrderDate', to_date(col('OrderDate'), "M/d/yyyy"))

# Calculate the average number of orders placed in a day based on the customer
window_spec = Window.partitionBy("CustomerKey")
orders_per_day_df = sales_datatype_df.groupBy("CustomerKey", "OrderDate").agg(countDistinct("OrderNumber").alias("NumOrders"))
avg_orders_per_day_df = orders_per_day_df.withColumn("AvgOrdersPerDay", avg(col("NumOrders")).over(window_spec))

avg_orders_per_day_df.show()

#stop the sparksession
spark.stop()

