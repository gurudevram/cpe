from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create SparkSession
spark = SparkSession.builder.master("local").appName("cpe").getOrCreate()

# Define file paths
data_dir = "D:\Gurudev\products_json"
CUSTOMERS = f"{data_dir}/customer_updated.json"
PRODUCTS = f"{data_dir}/products.json"
SALES = f"{data_dir}/sales.json"

# Define schemas for data
customer_schema = StructType([
    StructField('CustomerKey', IntegerType(), nullable=True),
    StructField('Prefix', StringType(), nullable=True),
    StructField('FirstName', StringType(), nullable=True),
    StructField('LastName', StringType(), nullable=True),
    StructField('BirthDate', StringType(), nullable=True),
    StructField('MaritalStatus', StringType(), nullable=True),
    StructField('Gender', StringType(), nullable=True),
    StructField('EmailAddress', StringType(), nullable=True),
    StructField('AnnualIncome', StringType(), nullable=True),
    StructField('TotalChildren', IntegerType(), nullable=True),
    StructField('EducationLevel', StringType(), nullable=True),
    StructField('Occupation', StringType(), nullable=True),
    StructField('HomeOwner', StringType(), nullable=True),
    StructField('PhoneNumber', IntegerType(), nullable=True),
    StructField('Address', StringType(), nullable=True),
])

product_schema = StructType([
    StructField('ProductKey', IntegerType(), nullable=True),
    StructField('ProductSubcategoryKey', IntegerType(), nullable=True),
    StructField('ProductName', StringType(), nullable=True),
    StructField('ProductDescription', StringType(), nullable=True),
    StructField('ProductColor', StringType(), nullable=True),
    StructField('ProductSize', StringType(), nullable=True),
    StructField('ProductCost', DoubleType(), nullable=True),
    StructField('ProductPrice', DoubleType(), nullable=True)
])

sales_schema = StructType([
    StructField('OrderDate', StringType(), nullable=True),
    StructField('StockDate', StringType(), nullable=True),
    StructField('OrderNumber', StringType(), nullable=True),
    StructField('ProductKey', IntegerType(), nullable=True),
    StructField('CustomerKey', IntegerType(), nullable=True),
    StructField('TerritoryKey', IntegerType(), nullable=True),
    StructField('OrderLineItem', IntegerType(), nullable=True),
    StructField('OrderQuantity', IntegerType(), nullable=True),
])

init_df = spark.read.format("csv").option("header", "true").load("D:/Gurudev/salesdataset/customer_updated_dataset.csv")
productsinit_df = spark.read.format("csv").option("header", "true").load("D:/Gurudev/salesdataset/AdventureWorksProducts-210509-235702.csv")
salesinit_df = spark.read.format("csv").option("header", "true").load("D:/Gurudev/salesdataset/AdventureWorksSales2015-210509-235702.csv")

# Read the data as JSON
customer_df = spark.read.option("multiline", "true").json(CUSTOMERS)
products_df = spark.read.option("multiline", "true").json(PRODUCTS)
sales_df = spark.read.option("multiline", "true").json(SALES)


##################### RAW LAYER ################################

# Deduplication
customer_null_df = customer_df.dropna()
customer_dedup_df = customer_null_df.dropDuplicates()
# Full name of customer
customer_name_df = customer_dedup_df.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))

##################### CLEANSED LAYER ################################

# Encrypt sensitive customer information fields
customer_encrypt_df=customer_name_df.withColumn('FullName', col('FullName').cast(StringType())) \
                     .withColumn('EmailAddress', col('EmailAddress').cast(StringType())) \
                     .withColumn('PhoneNumber', col('PhoneNumber').cast(StringType())) \
                     .withColumn('Address', col('Address').cast(StringType())) \


customer_encrypt_df=customer_name_df.withColumn('FullName', sha2(col('FullName'), 256)) \
              .withColumn('EmailAddress', sha2(col('EmailAddress'), 256)) \
              .withColumn('PhoneNumber', sha2(col('PhoneNumber'), 256)) \
              .withColumn('Address', sha2(col('Address'), 256)) \
              .drop("FirstName","LastName","Prefix","Gender")
# Changing data types
customer_datatype_df = customer_encrypt_df.withColumn('BirthDate', to_date(col('BirthDate'), "M/d/yyyy"))
sales_datatype_df = sales_df.withColumn('OrderDate', to_date(col('OrderDate'), "M/d/yyyy"))

##################### CURATED LAYER ################################

# Calculate the average number of orders placed in a day based on the customer
window_spec = Window.partitionBy("CustomerKey")
orders_per_day_df = sales_datatype_df.groupBy("CustomerKey", "OrderDate").agg(countDistinct("OrderNumber").alias("NumOrders"))
avg_orders_per_day_df = orders_per_day_df.withColumn("AvgOrdersPerDay", avg(col("NumOrders")).over(window_spec))

customer_datatype_df.show()
avg_orders_per_day_df.show()
