from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, window, col, lag, unix_timestamp, when, sum, count, min, max, mean, stddev
from pyspark.sql import Window
import matplotlib.pyplot as plt
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.driver.bindAddress", "127.0.0.1")
conf.set("spark.ui.port", "4050")

spark = SparkSession.builder \
    .appName("HadoopLogAnalysis") \
    .config(conf=conf) \
    .getOrCreate()

logfile_path = "/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/data/Hadoop_2k.log"
logs_df = spark.read.text(logfile_path)

timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'

structured_logs_df = logs_df \
    .withColumn("timestamp", regexp_extract("value", timestamp_pattern, 1)) \
    .withColumn("log_level", regexp_extract("value", log_level_pattern, 1)) \
    .withColumn("message", regexp_extract("value", message_pattern, 1))

log_level_count = structured_logs_df.groupby("log_level").count().toPandas()

print("---------------------------------------------------")
print("                 Distribution of Log Levels                    ")
print("---------------------------------------------------")
plt.bar(log_level_count["log_level"], log_level_count["count"], color="skyblue")
plt.xlabel("Log Level")
plt.ylabel("Count")
plt.title("Distribution of Log Levels")
plt.show()

print("---------------------------------------------------")
print("                 Errors Over Time                    ")
print("---------------------------------------------------")
logs_over_time = structured_logs_df.groupBy(window("timestamp", "1 minute"), "log_level").count()
logs_over_time = logs_over_time.withColumn("window_start", col("window.start"))
error_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "ERROR")
error_logs_over_time_pd = error_logs_over_time.select("window_start", "count").toPandas()
plt.figure(figsize=(10, 6))
plt.plot(
    error_logs_over_time_pd["window_start"],
    error_logs_over_time_pd["count"],
    marker="o",
    color="red"
)
plt.xlabel("Time (1-minute intervals)")
plt.ylabel("Error Count")
plt.title("Error Logs Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

print("---------------------------------------------------")
print("                 Warnings Over Time                    ")
print("---------------------------------------------------")
warning_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "WARN")
warning_logs_over_time_pd = warning_logs_over_time.select("window_start", "count").toPandas()
plt.figure(figsize=(10, 6))
plt.plot(
    warning_logs_over_time_pd["window_start"],
    warning_logs_over_time_pd["count"],
    marker="o",
    color="blue"
)
plt.xlabel("Time (1-minute intervals)")
plt.ylabel("Warning Count")
plt.title("Warning Logs Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

print("---------------------------------------------------")
print("                 Frequent Error Messages                    ")
print("---------------------------------------------------")
frequent_errors = structured_logs_df.filter(col("log_level") == "ERROR") \
    .groupBy("message") \
    .count() \
    .orderBy(col("count").desc())
frequent_errors.show(10, truncate=False)

print("---------------------------------------------------")
print("                 Filter Logs By Specific Keywords                   ")
print("---------------------------------------------------")
keyword_logs = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)Registering"))
keyword_logs.show(10, truncate=False)

print("---------------------------------------------------")
print("                 Most Common Keywords                   ")
print("---------------------------------------------------")
frequent_messages = structured_logs_df.groupby("message").count().orderBy("count", ascending=False)
frequent_messages.show(10, truncate=False)
frequent_messages_pd = frequent_messages.limit(10).toPandas()
plt.barh(frequent_messages_pd["message"], frequent_messages_pd["count"], color="skyblue")
plt.xlabel("Count")
plt.ylabel("Message")
plt.title("Top 10 Frequent Messages")
plt.tight_layout()
plt.show()

frequent_messages_pd["short_message"] = frequent_messages_pd["message"].str.slice(0, 50) + "..."
frequent_messages_pd = frequent_messages_pd.sort_values(by="count", ascending=False)
plt.figure(figsize=(10, 6))
plt.barh(
    frequent_messages_pd["short_message"],
    frequent_messages_pd["count"],
    color="blue"
)
plt.xlabel("Count")
plt.ylabel("Message")
plt.title("Top 10 Frequent Messages")
plt.tight_layout()
plt.show()

print("---------------------------------------------------")
print("                 Analyze Failure Patterns                   ")
print("---------------------------------------------------")
failures = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)failures"))
failure_details = failures.groupBy("message").count().orderBy("count", ascending=False)
failure_details.show(10, truncate=False)

print("---------------------------------------------------")
print("                 Correlate Failure Types with Timestamps                   ")
print("---------------------------------------------------")

lease_failures = structured_logs_df.filter(
    structured_logs_df.message.rlike("(?i)Failed to renew lease")
)
print("Number of lease renewal failures:", lease_failures.count())

lease_failures.show(10, truncate=False)

combined_failures = lease_failures.union(failures)
combined_failures.show(truncate=False)
combined_failures_grouped = combined_failures.groupBy(window("timestamp", "1 minute")).count()
combined_failures_pd = combined_failures_grouped.toPandas()
plt.figure(figsize=(10, 6))
plt.plot(
    combined_failures_pd["window"].apply(lambda x: x.start),
    combined_failures_pd["count"],
    marker="o",
    color="purple",
    label="Combined Failures"
)
plt.xlabel("Time (1-minute intervals)")
plt.ylabel("Failures Count")
plt.title("Combined Failures Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.legend()
plt.show()

print("---------------------------------------------------")
print("                 Anamoly Detection for Failures                   ")
print("---------------------------------------------------")
stats = logs_over_time.groupBy("log_level").agg(
    mean("count").alias("mean"), stddev("count").alias("stddev")
)
stats.show()
anomalies = logs_over_time.join(stats, "log_level").filter(
    (col("count") > col("mean") + 2 * col("stddev")) |
    (col("count") < col("mean") - 2 * col("stddev"))
)
anomalies.show(truncate=False)

print("---------------------------------------------------")
print("                 Sessionaization                   ")
print("---------------------------------------------------")
logs_with_prev_time = structured_logs_df.withColumn(
    "prev_timestamp", lag("timestamp").over(Window.orderBy("timestamp"))
)
partitioned_window = Window.partitionBy("log_level").orderBy("timestamp")
logs_with_prev_time = logs_with_prev_time.withColumn(
    "prev_timestamp", lag("timestamp").over(partitioned_window)
)
logs_with_prev_time = logs_with_prev_time.withColumn(
    "time_diff",
    unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
)
logs_with_sessions = logs_with_prev_time.withColumn(
    "session_id",
    sum(when(col("time_diff") > 60, 1).otherwise(0))
    .over(Window.orderBy("timestamp"))
)
logs_with_sessions.select("timestamp", "prev_timestamp", "time_diff", "session_id", "log_level", "message").show(10, truncate=False)
session_summary = logs_with_sessions.groupBy("session_id").agg(
    count("*").alias("log_count"),
    min("timestamp").alias("session_start"),
    max("timestamp").alias("session_end")
)
session_summary.show(truncate=False)
session_summary_pd = session_summary.toPandas()
plt.figure(figsize=(8, 6))
plt.bar(session_summary_pd["session_id"], session_summary_pd["log_count"], color="skyblue")
plt.xlabel("Session ID")
plt.ylabel("Log Count")
plt.title("Number of Logs per Session")
plt.tight_layout()
plt.show()
logs_with_sessions.groupBy("session_id", "log_level").count().show()
import os

output_dir = "/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
session_summary.write.csv(f"{output_dir}/session_summary.csv", header=True)

spark.stop()


