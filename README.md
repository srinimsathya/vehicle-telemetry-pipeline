
# 🚗 Vehicle Telemetry Data Pipeline

This is an end-to-end **real-time streaming data pipeline project** that simulates vehicle telemetry data (e.g., speed, location, engine temperature), streams it through **Apache Kafka**, stores it as JSON files, uploads to **Azure Data Lake Storage Gen2 (ADLS Gen2)**, processes it with **Azure Databricks**, stores it in **Delta Lake**, and visualizes it in **Power BI**.

---

## 📌 Project Architecture

```
Vehicle Simulator (Python Producer) 
       ⬇
Apache Kafka (local)
       ⬇
Python Consumer → JSON Files
       ⬇
Azure ADLS Gen2 (Blob Storage)
       ⬇
Azure Databricks (Delta Lake Table)
       ⬇
Power BI (Realtime Dashboard)
```

---

## 🛠️ Tech Stack

| Component           | Technology Used                     |
|---------------------|--------------------------------------|
| Data Simulation     | Python                              |
| Messaging System    | Apache Kafka (local setup)          |
| Storage             | Azure Data Lake Gen2 (Blob Storage) |
| Processing Engine   | Azure Databricks (PySpark + Delta)  |
| Orchestration       | Manual (can be extended via ADF)    |
| Visualization       | Power BI Desktop                    |

---

## 📁 Folder Structure

```
vehicle-telemetry-pipeline/
│
├── data/                          # JSON output files from consumer
├── PowerBI_Screen_Shots/         # Power BI dashboard screenshots
├── vehicle_producer.py           # Kafka producer script
├── vehicle_consumer.py           # Kafka consumer script (writes to JSON)
├── upload_to_adls_key.py         # Script to upload JSON to ADLS (access key method)
├── Vechile Telemetry Data Transform.py  # Databricks notebook logic (PySpark)
└── README.md                     # Project documentation
```

---

## 🔄 End-to-End Workflow

### ✅ Step 1: Start Kafka Locally
```bash
# Start Zookeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# Start Kafka Broker
bin\windows\kafka-server-start.bat config\server.properties

# Create Topic
bin\windows\kafka-topics.bat --create --topic vehicle-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### ✅ Step 2: Run Producer and Consumer Scripts
```bash
python vehicle_producer.py   # Simulates and sends data to Kafka
python vehicle_consumer.py   # Consumes and stores as JSON
```

### ✅ Step 3: Upload JSON to ADLS Gen2
```bash
python upload_to_adls_key.py   # Pushes JSON files to Azure Data Lake using access key
```

### ✅ Step 4: Transform Data in Azure Databricks
- **Authenticate using access key**
```python
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)
```
- **Read JSON from ADLS**
```python
df = spark.read.json("abfss://<container>@<account>.dfs.core.windows.net/vehicle_data_current.json")
```
- **Clean & cast timestamp**
```python
df_cleaned = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
```
- **Write to Delta format**
```python
df_cleaned.write.format("delta").mode("overwrite").save("abfss://<container>@<account>.dfs.core.windows.net/<delta-folder-name>/")
```

### ✅ Step 5: Unity Catalog Table Creation
```sql
-- CATALOG
CREATE CATALOG your_catalog_name
MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/<delta-folder-name>/';
-- SCHEMA
CREATE SCHEMA IF NOT EXISTS your_catalog_name.your_schema_name;
-- TABLE
CREATE TABLE IF NOT EXISTS your_catalog_name.your_schema_name.vehicle_telemetry
USING DELTA
LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/<delta-folder-name>/';
-- READ
SELECT * FROM your_catalog_name.your_schema_name.vehicle_telemetry;
```

### ✅ Step 6: Power BI Visualization
- Install Simba Spark ODBC driver
- Connect Power BI to Databricks SQL endpoint
- Load `vehicle_telemetry` table and create dashboard

---

## 📸 Sample Screenshots

See the `PowerBI_Screen_Shots/` folder for:
- Kafka terminal output
- JSON saved files
- ADLS upload confirmation
- Databricks transformations
- Delta table preview
- Power BI dashboard

---

## ✅ Features

- 🚘 Real-time vehicle data generation and ingestion
- 🧪 PySpark data cleaning and Delta Lake storage
- ☁️ Cloud-scale integration with ADLS and Databricks
- 📊 Power BI visualization for business insights

---

## 📬 Contact

**Srinivasan M**  
🔗 [LinkedIn](https://www.linkedin.com/in/srinimsathya/) | 💻 [GitHub](https://github.com/srinimsathya)
