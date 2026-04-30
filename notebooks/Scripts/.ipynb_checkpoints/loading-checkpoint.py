import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('Snowflake_Load_Layer') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m")\
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.22") \
    .getOrCreate()

sfOptions = {
    "sfURL": "kv86756.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "ashraqat",
    "sfPassword": "ft84x*NkSi?123",
    "sfDatabase": "TITANIC_DB",
    "sfSchema": "TITANIC_SCHEMA",
    "sfWarehouse": "MY_WH",
    "sfRole": "ACCOUNTADMIN"
}

gold_path = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

fact = spark.read.parquet(f"{gold_path}fact_passenger")
dim_passenger = spark.read.parquet(f"{gold_path}dim_passenger")
dim_embarked = spark.read.parquet(f"{gold_path}dim_embarked")
dim_class = spark.read.parquet(f"{gold_path}dim_class")

fact.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "FACT_PASSENGER") \
    .mode("overwrite") \
    .save()

dim_passenger.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_PASSENGER") \
    .mode("overwrite") \
    .save()

dim_embarked.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_EMBARKED") \
    .mode("overwrite") \
    .save()

dim_class.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_CLASS") \
    .mode("overwrite") \
    .save()