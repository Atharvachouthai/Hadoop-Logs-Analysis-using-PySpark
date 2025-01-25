# Hadoop-Logs-Analysis-using-PySpark
This repository contains a comprehensive project for analysing Hadoop logs using PySpark, with features for both batch processing and real-time streaming analysis. The goal is to gain insights into system logs, perform error/warning detection, and enable sessionization for better understanding and monitoring of log events.

Key Features:
	1.	Batch Log Analysis:
	  •	Sessionize log data based on time gaps.
	  •	Generate various insights such as:
	  •	Distribution of log levels (INFO, WARN, ERROR, etc.).
	  •	Error and warning trends over time.
	  •	Frequent error messages and log patterns.
	  •	Export sessionized data to a CSV file for further analysis.
	  •	Create insightful visualizations for log distributions, failures, and warnings.
	2.	Real-Time Streaming Analysis:
	  •	Use Spark Structured Streaming to process logs in real-time.
	  •	Analyze the count of log levels (INFO, WARN, ERROR) in a streaming environment.
	  •	Continuously query log-level distributions and print them in batches.

  Directory Structure:
	•	data/: Contains the input log file (Hadoop_2k.log).
	• output/: Stores the generated outputs such as:
	•	CSV files (e.g., session_summary.csv).
	•	Visualizations as PNG images (e.g., “Error Logs Over Time”).
	•	src/:
	•	main.py: Python script for batch log analysis.
	•	streaming_main.py: Python script for real-time streaming analysis.
	•	requirements.txt: List of dependencies for running the project.
	•	run_analysis.sh: Shell script to simplify running the analysis.

Technologies Used:
	•	Python: Core language for the implementation.
	•	PySpark: For distributed data processing and streaming.
	•	Matplotlib: For generating visualizations.
	•	Bash: Shell scripting for running the project efficiently.

 Example Use Cases:
	1. System Monitoring:
	  •	Analyze error and warning trends over time to identify system issues.
	2. Real-Time Alerts:
	  •	Set up streaming to detect and respond to critical errors in real-time.
	3. Log Summarization:
	  •	Sessionize logs to understand user sessions or application behavior.

  License:
  This project is open-source and licensed under MIT License.
 
