import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('BronzeLayerIngestion') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

print("Spark Connected")

input_path = "file:///home/jovyan/work/data/raw_events/"

df = spark.read.json(input_path)

df.show(5)
print(df.count())

output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"

df.write \
    .mode("append") \
    .format("parquet") \
    .save(output_path)

print("Data written to Bronze Layer in HDFS")
