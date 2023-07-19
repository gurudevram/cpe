from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder.master("local").appName("cpe").getOrCreate()

# Define file paths
data_dir = "D:\products_json"
CALENDAR = f"{data_dir}/calendar.json"
CUSTOMERS = f"{data_dir}/customer_updated.json"
PRODUCTS = f"{data_dir}/products.json"
PRODUCTSUBCATEGORIES = f"{data_dir}/product_subcategories.json"
RETURN = f"{data_dir}/returns.json"
SALES = f"{data_dir}/sales.json"
SALES_1 = f"{data_dir}/sales1.json"
TERRITORIES = f"{data_dir}/WorksTerritories.json"

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


# Read the data as JSON
customer_df = spark.read.option("multiline", "true").schema(customer_schema).json(CUSTOMERS)
products_df = spark.read.option("multiline", "true").schema(product_schema).json(PRODUCTS)
sales_df = spark.read.option("multiline", "true").schema(sales_schema).json(SALES)


products_df.write.json("hdfs://localhost:9000/wersdfxcv")
# df = spark.read.csv("hdfs://localhost:9000/path/part-00000-85f69aae-a206-4526-9ce1-1957b12623af-c000.csv", header=True, inferSchema=True)


# Cleansing the data
cleansed_customer_df = customer_df.fillna({"EmailAddress": ""}).withColumn("AnnualIncome", col("AnnualIncome").cast(IntegerType())).withColumn("TotalChildren", col("TotalChildren").cast(IntegerType()))
cleansed_product_df = products_df.fillna({"ProductColor": "", "ProductSize": ""})


curated_customer_df = cleansed_customer_df.withColumn("FullName", concat(col("FirstName"), lit(' '), col("LastName")))
curated_product_df = cleansed_product_df.withColumn("DiscountedPrice", col("ProductPrice") * 0.9)

customer_trans_df = customer_df.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName"))) \
    .withColumn("BirthDateFormatted", to_date(col("BirthDate"), "M/d/yyyy").cast(DateType())) \
    .withColumn("Age", datediff(current_date(), col("BirthDateFormatted")) / 365) \
    .drop("BirthDateFormatted")

customer_income_df = customer_trans_df.withColumn("IncomeCategory",
                                            when(col("AnnualIncome") < 50000, "Low")
                                            .when(col("AnnualIncome") < 100000, "Medium")
                                            .otherwise("High"))

product_profit_df = products_df.withColumn("ProductDescriptionLength", length(col("ProductDescription"))) \
    .withColumn("ProductProfit", col("ProductPrice") - col("ProductCost"))

product_size_df = products_df.withColumn("DiscountedPrice", col("ProductPrice") * 0.9) \
    .withColumn("ProductSizeCategory",
                when(col("ProductSize") == "Small", "S")
                .when(col("ProductSize") == "Medium", "M")
                .when(col("ProductSize") == "Large", "L")
                .otherwise("Unknown"))

# Deduplication
customer_null_df = customer_df.dropna()
product_null_df = products_df.dropna()

customer_dedup_df = customer_df.dropDuplicates()
product_dedup_df = products_df.dropDuplicates()
sales_dedup_df = sales_df.dropDuplicates()

customer_datatype_df = sales_dedup_df.withColumn("OrderDate", to_date(sales_dedup_df["OrderDate"], "yyyy-MM-dd"))

daily_orders = sales_dedup_df.groupBy("OrderDate").agg(countDistinct("OrderNumber").alias("num_orders"))

#average number of orders placed in a day

average_orders_per_day = daily_orders.selectExpr("avg(num_orders) as avg_orders_per_day").first()[0]

########Mask the sensitive customer information fields#####

# Create masking function
def mask_data(customer_trans_df):
    return "*" * len(customer_trans_df)

# Register UDF (User-Defined Function)
mask_data_udf = udf(mask_data, StringType())

# masked_data = customer_df.withColumn("masked_name", mask_data_udf("FullName")) \
#                            .withColumn("masked_email", mask_data_udf("EmailAddress")) \
#                            .withColumn("masked_address", mask_data_udf("Address")) \
#                            .withColumn("masked_phone", mask_data_udf("PhoneNumber"))

masked_data = customer_trans_df.withColumn("masked_name", mask_data_udf("FullName")) \
    .withColumn("masked_email", mask_data_udf("EmailAddress")) \
    .withColumn("masked_address", mask_data_udf("Address")) \
    .withColumn("masked_phone", mask_data_udf("PhoneNumber"))


# Get How many Orders were placed
orders_placed = sales_dedup_df.select("OrderNumber").distinct().count()

# Get Average Revenue Per Order
avg_revenue_per_order = sales_dedup_df.groupBy("OrderNumber").agg(sum("OrderQuantity")).agg(avg("sum(OrderQuantity)")).collect()[0][0]

# Get Average Revenue Per Day
avg_revenue_per_day = sales_dedup_df.groupBy("OrderDate").agg(sum("OrderQuantity")).agg(avg("sum(OrderQuantity)")).collect()[0][0]

# Get Average Revenue Per Month
avg_revenue_per_month = sales_dedup_df.withColumn("Month", month("OrderDate")).groupBy("Month").agg(sum("OrderQuantity")).agg(avg("sum(OrderQuantity)")).collect()[0][0]

# Get Total Revenue Per Month Per Year
total_revenue_per_month_per_year = sales_dedup_df.withColumn("Year", year("OrderDate")).withColumn("Month", month("OrderDate")).groupBy("Year", "Month").agg(sum("OrderQuantity")).orderBy("Year", "Month")

# Group Revenues per Month per Year
revenues_per_month_per_year = sales_dedup_df.withColumn("Year", year("OrderDate")).withColumn("Month", month("OrderDate")).groupBy("Year", "Month").agg(sum("OrderQuantity")).orderBy("Year", "Month")

# Get Highest Priced Product
highest_priced_product = product_dedup_df.orderBy(col("ProductPrice").desc()).first()

# Get Number of Orders By Order Date
orders_by_order_date = sales_dedup_df.groupBy("OrderDate").count().orderBy("OrderDate")

# Sort Products by Category
sorted_products_by_category = product_dedup_df.orderBy("ProductName")

# Show results
print("Orders Placed:", orders_placed)
print("Average Revenue Per Order:", avg_revenue_per_order)
print("Average Revenue Per Day:", avg_revenue_per_day)
print("Average Revenue Per Month:", avg_revenue_per_month)
print("Total Revenue Per Month Per Year:")
total_revenue_per_month_per_year.show()
print("Revenues per Month per Year:")
revenues_per_month_per_year.show()
print("Highest Priced Product:", highest_priced_product)
print("Number of Orders By Order Date:")
orders_by_order_date.show()
print("Sorted Products by Category:")
sorted_products_by_category.show()
masked_data.show()
print("Average orders placed in a day: {:.2f}".format(average_orders_per_day))
