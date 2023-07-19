from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
import json
import os

os.environ['HADOOP_HOME'] = "C:\\hadoop-3.3.0"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

kafka_topic = "CUSTOMERS_DATA"
kafka_server = "localhost:9092"
csv_file = r"C:\Users\gurudev.r\Downloads\salesdataset\customer_updated_dataset.csv"
output_path = "hdfs://localhost:9000/CPE"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka for CPE Application Started ...")

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("PySpark Structured Streaming for CPE") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read the CSV file into a DataFrame
    df = spark.read.format("csv").option("header", "true").load(csv_file)
    df.show()

    # Convert DataFrame to JSON and send records to Kafka topic
    json_records = df.toJSON().map(lambda x: json.loads(x)).collect()

    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_server)

    for record in json_records:
        message = json.dumps(record).encode('utf-8')
        producer.send(kafka_topic, value=message)

    # Close the producer
    producer.close()

    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Process the streaming DataFrame as needed
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), df.schema).alias("data")).select("data.*")

    # Write the processed DataFrame to HDFS as JSON files
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", "hdfs://localhost:9000/checkpoint") \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()

    # Stop the SparkSession
    spark.stop()









# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from kafka import KafkaProducer
# import os
#
# os.environ['HADOOP_HOME'] = "C:\\hadoop-3.3.0"
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
#
# kafka_topic = "CUSTOMERS_DATA"
# kafka_server = "localhost:9092"
# csv_file = r"C:\Users\gurudev.r\Downloads\salesdataset\AdventureWorksSales2015-210509-235702.csv"
# output_path = "hdfs://localhost:9000/categories"
#
# if __name__ == "__main__":
#     print("PySpark Structured Streaming with Kafka for CPE Application Started ...")
#
#     # Create SparkSession
#     spark = SparkSession.builder \
#         .appName("PySpark Structured Streaming for CPE") \
#         .master("local[*]") \
#         .getOrCreate()
#
#     spark.sparkContext.setLogLevel("ERROR")
#
#     # Read the CSV file into a DataFrame
#     df = spark.read.format("csv").option("header", "true").load(csv_file)
#     df.show()
#
#     # Create Kafka producer
#     producer = KafkaProducer(bootstrap_servers=kafka_server)
#
#     # Convert DataFrame to JSON and send records to Kafka topic
#     json_records = df.toJSON().collect()
#     for record in json_records:
#         message = record.encode('utf-8')
#         producer.send(kafka_topic, value=message)
#
#     # Close the producer
#     producer.close()
#
#     # Read from Kafka topic as a streaming DataFrame
#     kafka_df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_server) \
#         .option("subscribe", kafka_topic) \
#         .option("startingOffsets", "earliest") \
#         .load()
#
#     # Process the streaming DataFrame as needed
#     processed_df = kafka_df.selectExpr("CAST(value AS STRING)")
#
#     # Write the processed DataFrame to HDFS as JSON files
#     query = processed_df \
#         .writeStream \
#         .outputMode("append") \
#         .format("json") \
#         .option("path", output_path) \
#         .option("checkpointLocation", "hdfs://localhost:9000/checkpoint") \
#         .start()
#
#     # Wait for the streaming query to finish
#     query.awaitTermination()
#
#     # Stop the SparkSession
#     spark.stop()


