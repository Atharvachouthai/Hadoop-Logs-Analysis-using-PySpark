from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

spark = SparkSession.builder \
    .appName("RealTimeLogAnalysis") \
    .getOrCreate()

logs_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'
structured_logs_df = logs_df \
    .withColumn("timestamp", regexp_extract("value", timestamp_pattern, 1)) \
    .withColumn("log_level", regexp_extract("value", log_level_pattern, 1)) \
    .withColumn("message", regexp_extract("value", message_pattern, 1))

log_level_count = structured_logs_df.groupBy("log_level").count()

batch_counter = {"batch_number": 0}

def process_batch(batch_df, batch_id):
    batch_counter["batch_number"] += 1
    print("---------------------------------------------------")
    print(f"                 Batch {batch_counter['batch_number']}                   ")
    print("---------------------------------------------------")
    batch_df.show()

query = log_level_count.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()