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

returns_schema = StructType([
    StructField('ReturnDate', StringType(), nullable=True),
    StructField('TerritoryKey', IntegerType(), nullable=True),
    StructField('ProductKey', IntegerType(), nullable=True),
    StructField('ReturnQuantity', IntegerType(), nullable=True),
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


# Define file paths
data_dir = "D:\Gurudev\products_json"
CUSTOMERS = f"{data_dir}/customer_updated.json"
SALES = f"{data_dir}/sales.json"


customer_df = spark.read.option("multiline", "true").json(CUSTOMERS)
sales_df = spark.read.option("multiline", "true").json(SALES)

##################### RAW LAYER ################################

# Deduplication
customer_null_df = customer_df.dropna()
customer_dedup_df = customer_null_df.dropDuplicates()
# Full name of customer
customer_name_df = customer_dedup_df.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))

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

customer_datatype_df.show()

#stop the sparksession
spark.stop()

