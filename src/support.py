# from pyspark.sql import SparkSession
# from pyspark.sql.functions import regexp_extract, window, col, lag, unix_timestamp, when, sum, count, min, max, mean, stddev
# from pyspark.sql import Window
# import matplotlib.pyplot as plt
# from pyspark import SparkConf

# conf = SparkConf()
# conf.set("spark.driver.bindAddress", "127.0.0.1")
# conf.set("spark.ui.port", "4050")

# spark = SparkSession.builder \
#     .appName("HadoopLogAnalysis") \
#     .config(conf=conf) \
#     .getOrCreate()

# logfile_path = "/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/data/Hadoop_2k.log"
# logs_df = spark.read.text(logfile_path)

# timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
# log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
# message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'

# structured_logs_df = logs_df \
#     .withColumn("timestamp", regexp_extract("value", timestamp_pattern, 1)) \
#     .withColumn("log_level", regexp_extract("value", log_level_pattern, 1)) \
#     .withColumn("message", regexp_extract("value", message_pattern, 1))

# log_level_count = structured_logs_df.groupby("log_level").count().toPandas()

# print("---------------------------------------------------")
# print("                 Distribution of Log Levels                    ")
# print("---------------------------------------------------")
# plt.bar(log_level_count["log_level"], log_level_count["count"], color="skyblue")
# plt.xlabel("Log Level")
# plt.ylabel("Count")
# plt.title("Distribution of Log Levels")
# plt.show()

# print("---------------------------------------------------")
# print("                 Errors Over Time                    ")
# print("---------------------------------------------------")
# logs_over_time = structured_logs_df.groupBy(window("timestamp", "1 minute"), "log_level").count()
# logs_over_time = logs_over_time.withColumn("window_start", col("window.start"))
# error_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "ERROR")
# error_logs_over_time_pd = error_logs_over_time.select("window_start", "count").toPandas()
# plt.figure(figsize=(10, 6))
# plt.plot(
#     error_logs_over_time_pd["window_start"],
#     error_logs_over_time_pd["count"],
#     marker="o",
#     color="red"
# )
# plt.xlabel("Time (1-minute intervals)")
# plt.ylabel("Error Count")
# plt.title("Error Logs Over Time")
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# print("---------------------------------------------------")
# print("                 Warnings Over Time                    ")
# print("---------------------------------------------------")
# warning_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "WARN")
# warning_logs_over_time_pd = warning_logs_over_time.select("window_start", "count").toPandas()
# plt.figure(figsize=(10, 6))
# plt.plot(
#     warning_logs_over_time_pd["window_start"],
#     warning_logs_over_time_pd["count"],
#     marker="o",
#     color="blue"
# )
# plt.xlabel("Time (1-minute intervals)")
# plt.ylabel("Warning Count")
# plt.title("Warning Logs Over Time")
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# print("---------------------------------------------------")
# print("                 Frequent Error Messages                    ")
# print("---------------------------------------------------")
# frequent_errors = structured_logs_df.filter(col("log_level") == "ERROR") \
#     .groupBy("message") \
#     .count() \
#     .orderBy(col("count").desc())
# frequent_errors.show(10, truncate=False)

# print("---------------------------------------------------")
# print("                 Filter Logs By Specific Keywords                   ")
# print("---------------------------------------------------")
# keyword_logs = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)Registering"))
# keyword_logs.show(10, truncate=False)

# print("---------------------------------------------------")
# print("                 Most Common Keywords                   ")
# print("---------------------------------------------------")
# frequent_messages = structured_logs_df.groupby("message").count().orderBy("count", ascending=False)
# frequent_messages.show(10, truncate=False)
# frequent_messages_pd = frequent_messages.limit(10).toPandas()
# plt.barh(frequent_messages_pd["message"], frequent_messages_pd["count"], color="skyblue")
# plt.xlabel("Count")
# plt.ylabel("Message")
# plt.title("Top 10 Frequent Messages")
# plt.tight_layout()
# plt.show()

# frequent_messages_pd["short_message"] = frequent_messages_pd["message"].str.slice(0, 50) + "..."
# frequent_messages_pd = frequent_messages_pd.sort_values(by="count", ascending=False)
# plt.figure(figsize=(10, 6))
# plt.barh(
#     frequent_messages_pd["short_message"],
#     frequent_messages_pd["count"],
#     color="blue"
# )
# plt.xlabel("Count")
# plt.ylabel("Message")
# plt.title("Top 10 Frequent Messages")
# plt.tight_layout()
# plt.show()

# print("---------------------------------------------------")
# print("                 Analyze Failure Patterns                   ")
# print("---------------------------------------------------")
# failures = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)failures"))
# failure_details = failures.groupBy("message").count().orderBy("count", ascending=False)
# failure_details.show(10, truncate=False)

# print("---------------------------------------------------")
# print("                 Correlate Failure Types with Timestamps                   ")
# print("---------------------------------------------------")

# lease_failures = structured_logs_df.filter(
#     structured_logs_df.message.rlike("(?i)Failed to renew lease")
# )
# print("Number of lease renewal failures:", lease_failures.count())

# lease_failures.show(10, truncate=False)

# combined_failures = lease_failures.union(failures)
# combined_failures.show(truncate=False)
# combined_failures_grouped = combined_failures.groupBy(window("timestamp", "1 minute")).count()
# combined_failures_pd = combined_failures_grouped.toPandas()
# plt.figure(figsize=(10, 6))
# plt.plot(
#     combined_failures_pd["window"].apply(lambda x: x.start),
#     combined_failures_pd["count"],
#     marker="o",
#     color="purple",
#     label="Combined Failures"
# )
# plt.xlabel("Time (1-minute intervals)")
# plt.ylabel("Failures Count")
# plt.title("Combined Failures Over Time")
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.legend()
# plt.show()

# print("---------------------------------------------------")
# print("                 Anamoly Detection for Failures                   ")
# print("---------------------------------------------------")
# stats = logs_over_time.groupBy("log_level").agg(
#     mean("count").alias("mean"), stddev("count").alias("stddev")
# )
# stats.show()
# anomalies = logs_over_time.join(stats, "log_level").filter(
#     (col("count") > col("mean") + 2 * col("stddev")) |
#     (col("count") < col("mean") - 2 * col("stddev"))
# )
# anomalies.show(truncate=False)

# print("---------------------------------------------------")
# print("                 Sessionaization                   ")
# print("---------------------------------------------------")
# logs_with_prev_time = structured_logs_df.withColumn(
#     "prev_timestamp", lag("timestamp").over(Window.orderBy("timestamp"))
# )
# partitioned_window = Window.partitionBy("log_level").orderBy("timestamp")
# logs_with_prev_time = logs_with_prev_time.withColumn(
#     "prev_timestamp", lag("timestamp").over(partitioned_window)
# )
# logs_with_prev_time = logs_with_prev_time.withColumn(
#     "time_diff",
#     unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
# )
# logs_with_sessions = logs_with_prev_time.withColumn(
#     "session_id",
#     sum(when(col("time_diff") > 60, 1).otherwise(0))
#     .over(Window.orderBy("timestamp"))
# )
# logs_with_sessions.select("timestamp", "prev_timestamp", "time_diff", "session_id", "log_level", "message").show(10, truncate=False)
# session_summary = logs_with_sessions.groupBy("session_id").agg(
#     count("*").alias("log_count"),
#     min("timestamp").alias("session_start"),
#     max("timestamp").alias("session_end")
# )
# session_summary.show(truncate=False)
# session_summary_pd = session_summary.toPandas()
# plt.figure(figsize=(8, 6))
# plt.bar(session_summary_pd["session_id"], session_summary_pd["log_count"], color="skyblue")
# plt.xlabel("Session ID")
# plt.ylabel("Log Count")
# plt.title("Number of Logs per Session")
# plt.tight_layout()
# plt.show()
# logs_with_sessions.groupBy("session_id", "log_level").count().show()
# # session_summary.write.csv("/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/output/session_summary.csv", header=True)

# spark.stop()


from itertools import count
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import window
from pyspark.sql.functions import col 
import warnings
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set("spark.driver.bindAddress", "127.0.0.1")  # Bind the driver to localhost
conf.set("spark.ui.port", "4050")  # Optionally set a specific port for the Spark UI to avoid conflicts

spark = SparkSession.builder \
    .appName("HadoopLogAnalysis") \
    .config(conf=conf) \
    .getOrCreate()

logfile_path = "/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/data/Hadoop_2k.log"
logs_df = spark.read.text(logfile_path)

timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'

structured_logs_df = logs_df\
    .withColumn("timestamp", regexp_extract("value",timestamp_pattern,1)) \
    .withColumn("log_level", regexp_extract("value",log_level_pattern,1)) \
    .withColumn("message", regexp_extract("value", message_pattern,1))


pandas_df = structured_logs_df.limit(10).toPandas()  # Convert only the first 10 rows
# pandas_df
# structured_logs_df.show(10, truncate = False)
# spark.stop()

log_level_count = structured_logs_df.groupby("log_level").count()
# log_level_count.show()

log_level_count_pd = log_level_count.toPandas()
# log_level_count_pd

error_logs_df = structured_logs_df.filter(structured_logs_df.log_level == 'ERROR')
# error_logs_df.show()

error_logs_df_pd = error_logs_df.toPandas()
# error_logs_df_pd

# structured_logs_df.filter(structured_logs_df.log_level == "ERROR").select("value").show(truncate=False)

print("---------------------------------------------------")
print("                 Distribution of Log Levels                    ")
print("---------------------------------------------------")
log_level_count = structured_logs_df.groupby("log_level").count().toPandas()

plt.bar(log_level_count["log_level"],log_level_count["count"],color = "skyblue")
plt.xlabel("Log Level")
plt.ylabel("Count")
plt.title("Distribution of Log Levels")
plt.show()

print("---------------------------------------------------")
print("                 Errors Over Time                    ")
print("---------------------------------------------------")

logs_over_time = structured_logs_df.groupBy(window("timestamp","1 minute"),"log_level").count()

# Extract start time of the window for better visualization
logs_over_time = logs_over_time.withColumn("window_start", col("window.start"))

# Step 5: Filter for ERROR logs
error_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "ERROR")

# Convert to Pandas for visualization
error_logs_over_time_pd = error_logs_over_time.select("window_start", "count").toPandas()

# Step 6: Plot the data
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
# plt.show()

print("---------------------------------------------------")
print("                 Warnings Over Time                    ")
print("---------------------------------------------------")

logs_over_time = structured_logs_df.groupBy(window("timestamp","1 minute"),"log_level").count()

# Extract start time of the window for better visualization
logs_over_time = logs_over_time.withColumn("window_start", col("window.start"))

# Step 5: Filter for ERROR logs
warning_logs_over_time = logs_over_time.filter(logs_over_time.log_level == "WARN")

# Convert to Pandas for visualization
warning_logs_over_time_pd = warning_logs_over_time.select("window_start", "count").toPandas()

# Step 6: Plot the data
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
# plt.show()


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

print("---------------------------------------------------")
print("                 Filter Logs By Specific Keywords                   ")
print("---------------------------------------------------")

frequent_messages_pd = frequent_messages.limit(10).toPandas()

plt.barh(frequent_messages_pd["message"], frequent_messages_pd["count"], color="skyblue")
plt.xlabel("Count")
plt.ylabel("Message")
plt.title("Top 10 Frequent Messages")
plt.tight_layout()
plt.show()

print(frequent_messages_pd.head())

# Limit the message length for better visualization
frequent_messages_pd["short_message"] = frequent_messages_pd["message"].str.slice(0, 50) + "..."

# Sort by count to ensure the bars are in order
frequent_messages_pd = frequent_messages_pd.sort_values(by="count", ascending=False)

# Plot the bar chart
plt.figure(figsize=(10, 6))
plt.barh(
    frequent_messages_pd["short_message"],  # Shortened message for x-axis
    frequent_messages_pd["count"],         # Count for y-axis
    color="blue"
)
plt.xlabel("Count")
plt.ylabel("Message")
plt.title("Top 10 Frequent Messages")
plt.tight_layout()
plt.show()


print("---------------------------------------------------")
print("                 Anamoly Detection                   ")
print("---------------------------------------------------")
# Display unique messages for analysis
# structured_logs_df.select("message").distinct().show(20, truncate=False)

# Filter lease renewal failures
lease_failures = structured_logs_df.filter(
    structured_logs_df.message.rlike("(?i)Failed to renew lease")
)

# Count the number of lease failures
print("Number of lease renewal failures:", lease_failures.count())

# Show a sample of lease failures
lease_failures.show(10, truncate=False)



# Filter logs containing the word "failures"
failures = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)failures"))

# Group failures by log_level and count
failure_summary = failures.groupBy("log_level").count()
failure_summary.show()

# Show failure logs
failures.show(10, truncate=False)

from pyspark.sql.functions import when

# Categorize logs based on keywords
categorized_logs = structured_logs_df.withColumn(
    "category",
    when(structured_logs_df.message.rlike("(?i)Progress of TaskAttempt"), "Task Progress")
    .when(structured_logs_df.message.rlike("(?i)Failed to renew lease"), "Lease Renewal Failure")
    .when(structured_logs_df.message.rlike("(?i)failures"), "Failure")
    .when(structured_logs_df.message.rlike("(?i)KILLING"), "Task Killing")
    .otherwise("Other")
)

# Group by category and count
category_summary = categorized_logs.groupBy("category").count()
category_summary.show()

category_summary_pd = categorized_logs.groupBy("category").count().toPandas()

# Bar chart
plt.figure(figsize=(8, 6))
plt.bar(category_summary_pd["category"], category_summary_pd["count"], color="skyblue")
plt.xlabel("Log Categories")
plt.ylabel("Count")
plt.title("Log Categories Distribution")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


lease_failures_over_time = lease_failures.groupBy(
    window("timestamp", "1 minute")
).count()

# Convert to Pandas for visualization
lease_failures_pd = lease_failures_over_time.toPandas()

# Line chart
plt.figure(figsize=(10, 6))
plt.plot(
    lease_failures_pd["window"].apply(lambda x: x.start),  # Extract start of the window
    lease_failures_pd["count"],
    marker="o",
    color="red",
    label="Lease Renewal Failures"
)
plt.xlabel("Time (1-minute intervals)")
plt.ylabel("Failures Count")
plt.title("Lease Renewal Failures Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.legend()
plt.show()


print("---------------------------------------------------")
print("                 Integration with Spark SQL                   ")
print("---------------------------------------------------")

structured_logs_df.createOrReplaceTempView("logs")
spark.sql("SELECT log_level, COUNT(*) FROM logs GROUP BY log_level").show()

print("---------------------------------------------------")
print("                 Analyze Failure Patterns                   ")
print("---------------------------------------------------")
failures = structured_logs_df.filter(structured_logs_df.message.rlike("(?i)failures"))
failure_details = failures.groupBy("message").count().orderBy("count", ascending=False)

failure_details.show(10, truncate=False)

print("---------------------------------------------------")
print("                 Correlate Failure Types with Timestamps                   ")
print("---------------------------------------------------")

combined_failures = lease_failures.union(failures)
combined_failures.show(truncate=False)

combined_failures_grouped = combined_failures.groupBy(window("timestamp", "1 minute")).count()
combined_failures_pd = combined_failures_grouped.toPandas()

# Plot combined failures over time
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

from pyspark.sql.functions import mean, stddev, col

# Calculate mean and standard deviation of failures
stats = logs_over_time.groupBy("log_level").agg(
    mean("count").alias("mean"), stddev("count").alias("stddev")
)
stats.show()

# Mark anomalies
anomalies = logs_over_time.join(stats, "log_level").filter(
    (col("count") > col("mean") + 2 * col("stddev")) |
    (col("count") < col("mean") - 2 * col("stddev"))
)
anomalies.show(truncate=False)

print("---------------------------------------------------")
print("                 Sessionaization                   ")
print("---------------------------------------------------")

from pyspark.sql import Window
from pyspark.sql.functions import lag, col, unix_timestamp, when, sum

# Add a previous timestamp column to calculate time differences
logs_with_prev_time = structured_logs_df.withColumn(
    "prev_timestamp", lag("timestamp").over(Window.orderBy("timestamp"))
)
partitioned_window = Window.partitionBy("log_level").orderBy("timestamp")
logs_with_prev_time = logs_with_prev_time.withColumn(
    "prev_timestamp", lag("timestamp").over(partitioned_window)
)

# Calculate the time difference (in seconds) between consecutive logs
logs_with_prev_time = logs_with_prev_time.withColumn(
    "time_diff",
    unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
)

# Create a session ID by marking where the time difference exceeds the threshold (e.g., 300 seconds)
logs_with_sessions = logs_with_prev_time.withColumn(
    "session_id",
    sum(when(col("time_diff") > 60, 1).otherwise(0))
    .over(Window.orderBy("timestamp"))
)

# Show the logs with session IDs
logs_with_sessions.select("timestamp", "prev_timestamp", "time_diff", "session_id", "log_level", "message").show(10, truncate=False)
from pyspark.sql.functions import count, min, max
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
session_summary.write.csv("/Users/atharvachouthai/Desktop/projects/HadoopLogsAnalysis/output/session_summary.csv", header=True)




# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import regexp_extract, window, col
# # from pyspark.sql import SparkSession

# # spark = SparkSession.builder \
# #     .appName("RealTimeLogAnalysis") \  # Make sure the app name is properly spelled
# #     .config("spark.sql.shuffle.partitions", "2") \
# #     .getOrCreate()

# # logs_df = spark.readStream \
# #     .format("socket") \
# #     .option("host", "localhost") \
# #     .option("port", "9999") \
# #     .load()

# # timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
# # log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
# # message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'

# # structured_logs_df = logs_df \
# #     .withColumn("timestamp", regexp_extract("value", timestamp_pattern, 1)) \
# #     .withColumn("log_level", regexp_extract("value", log_level_pattern, 1)) \
# #     .withColumn("message", regexp_extract("value", message_pattern, 1))

# # log_level_count = structured_logs_df.groupBy("log_level").count()

# # query = log_level_count.writeStream \
# #     .outputMode("complete") \
# #     .format("console") \
# #     .start()

# # query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import regexp_extract, window, col

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("RealTimeLogAnalysis") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .getOrCreate()

# # Define the streaming source (socket)
# logs_df = spark.readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # Define regex patterns for log components
# timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
# log_level_pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'
# message_pattern = r'(?:INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\s(?:\[.*?\]\s)?(.*)'

# # Extract structured data from raw logs
# structured_logs_df = logs_df \
#     .withColumn("timestamp", regexp_extract("value", timestamp_pattern, 1)) \
#     .withColumn("log_level", regexp_extract("value", log_level_pattern, 1)) \
#     .withColumn("message", regexp_extract("value", message_pattern, 1))

# # Calculate log level distribution
# log_level_count = structured_logs_df.groupBy("log_level").count()

# # Write results to a memory sink
# query = log_level_count.writeStream \
#     .outputMode("complete") \
#     .format("memory") \
#     .queryName("log_levels") \
#     .start()

# # Continuously query the in-memory table for updates
# import time
# while True:
#     spark.sql("SELECT * FROM log_levels").show()
#     time.sleep(5)  

# # # Define a sink to output results to the console
# # query = log_level_count.writeStream \
# #     .outputMode("complete") \
# #     .format("console") \
# #     .start()

# # # Wait for the streaming query to finish
# # query.awaitTermination()