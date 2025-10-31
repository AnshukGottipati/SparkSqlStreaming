# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── CSV files of task 1.
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Instructions:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Print the parsed data to the console (using .writeStream.format("console")).

## **Explanations, Approach, and Output:**
1. Parsed json response from dataGen into tables 
2. Stored console information to output.txt in the task_1 directory

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-------+---------+-----------+-----------+---------+
|trip_id|driver_id|distance_km|fare_amount|timestamp|
+-------+---------+-----------+-----------+---------+
+-------+---------+-----------+-----------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------+---------+-----------+-----------+-------------------+
|trip_id                             |driver_id|distance_km|fare_amount|timestamp          |
+------------------------------------+---------+-----------+-----------+-------------------+
|63f0ac3a-fd0b-46d7-b8ad-8495581ec80e|10       |31.04      |116.32     |2025-10-31 17:08:09|
|d6d759bd-ddfb-44db-ae21-1d3c71d56b7a|54       |37.1       |97.48      |2025-10-31 17:08:10|
+------------------------------------+---------+-----------+-----------+-------------------+

```
---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Instructions:**
1. Reuse the parsed DataFrame from Task 1.
2. Group by driver_id and compute:
3. SUM(fare_amount) as total_fare
4. AVG(distance_km) as avg_distance
5. Store the result in csv

### **Explanations, Approach, and Output**
1. Calculating the total fare amount and avg trip driven by each driver
2. Aggregate the information and storing it as a csv for each batch

```
driver_id,total_fare,avg_distance
7,41.26,49.4
73,138.95,49.89
16,71.74,13.26
5,52.32,19.62
41,135.31,24.305
1,70.57,24.19
37,141.12,21.06
83,125.72,5.38
91,43.66,9.72

```
---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

### **Explanations, Approach, and Output**
1. Watermark to handle incoming late data
2. computing sum of fare for a driver in a 5 min period
3. Had to use update instead of append for the query to show partial aggregates.

```
driver_id,window_start,window_end,window_total_fare
56,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,86.5
93,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,196.7
22,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,11.4
81,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,108.29
93,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,196.7
93,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,196.7
7,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,248.12
96,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,206.64
56,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,194.79000000000002
7,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,239.06
56,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,315.89
56,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,315.89
56,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,315.89
7,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,44.43
7,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,239.06
7,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,239.06
81,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,108.29
93,2025-10-31T17:59:00.000Z,2025-10-31T18:04:00.000Z,196.7
96,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,278.66
22,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,11.4
81,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,108.29
22,2025-10-31T17:56:00.000Z,2025-10-31T18:01:00.000Z,296.28
81,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,108.29
22,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,50.44
22,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,50.44
96,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,206.64
96,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,80.48
96,2025-10-31T17:57:00.000Z,2025-10-31T18:02:00.000Z,206.64
81,2025-10-31T18:00:00.000Z,2025-10-31T18:05:00.000Z,108.29
93,2025-10-31T17:58:00.000Z,2025-10-31T18:03:00.000Z,196.7


```
---

## 📬 Submission Checklist

- [x] Python scripts 
- [x] Output files in the `outputs/` directory  
- [x] Completed `README.md`  
- [x] Commit everything to GitHub Classroom  
- [x] Submit your GitHub repo link on canvas

---

