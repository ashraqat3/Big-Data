import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('Titanic') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m")\
    .getOrCreate()

bronze_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
gold_path = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

df = spark.read.parquet(bronze_path)

df.show(5)
print(df.count())

df = df.dropDuplicates()

from pyspark.sql.types import DoubleType

df = df.withColumn("Age", F.col("Age").cast(DoubleType()))
age_median = df.approxQuantile("Age", [0.5], 0.01)[0]
df = df.fillna({"Age": age_median})

df = df.fillna({"Embarked": "S"})

fare_mean = df.select(F.mean("Fare")).collect()[0][0]
df = df.fillna({"Fare": fare_mean})

df = df.drop("Cabin")

from pyspark.sql.types import IntegerType, DoubleType
df = df.withColumn("Age", df["Age"].cast(DoubleType())) \
       .withColumn("Fare", df["Fare"].cast(DoubleType())) \
       .withColumn("Survived", df["Survived"].cast(IntegerType())) \
       .withColumn("Pclass", df["Pclass"].cast(IntegerType()))

df = df.withColumn("Sex", F.lower(F.col("Sex"))) \
       .withColumn("Embarked", F.upper(F.col("Embarked")))

df = df.withColumn(
    "AgeGroup",
    F.when(df.Age < 18, "Child")
     .when(df.Age < 60, "Adult")
     .otherwise("Senior")
)

df = df.drop("Ticket")

df.printSchema()
df.show(5)
print("Final Count:", df.count())

df = df.withColumn(
    "FamilySize",
    F.col("SibSp").cast("int") + F.col("Parch").cast("int") + F.lit(1)
)

df = df.withColumn(
    "IsAlone",
    F.when(F.col("FamilySize") == 1, 1).otherwise(0)
)

df = df.withColumn(
    "FarePerPerson",
    F.col("Fare") / F.col("FamilySize")
)

df = df.withColumn(
    "AgeBucket",
    F.when(F.col("Age") < 12, "Child")
     .when(F.col("Age") < 18, "Teen")
     .when(F.col("Age") < 60, "Adult")
     .otherwise("Senior")
)

df = df.withColumn("Sex", F.lower(F.col("Sex"))) \
       .withColumn("Embarked", F.upper(F.col("Embarked")))

df = df.withColumnRenamed("SibSp", "SiblingsSpouses") \
       .withColumnRenamed("Parch", "ParentsChildren")

sex_survival = df.groupBy("Sex").agg(
    F.count("*").alias("Total"),
    F.sum("Survived").alias("Survived"),
    (F.sum("Survived") / F.count("*")).alias("SurvivalRate")
)

class_survival = df.groupBy("Pclass").agg(
    F.count("*").alias("Total"),
    F.sum("Survived").alias("Survived")
)

age_survival = df.groupBy("AgeBucket").agg(
    F.count("*").alias("Total"),
    F.sum("Survived").alias("Survived")
)

fact_passenger = df.select(
    "PassengerId",
    "Survived",
    "Pclass",
    "Fare",
    "FarePerPerson",
    "FamilySize",
    "IsAlone",
    "Age"
)

dim_passenger = df.select(
    "PassengerId",
    "Name",
    "Sex",
    "AgeBucket"
)

dim_embarked = df.select(
    "PassengerId",
    "Embarked"
)

dim_class = df.select(
    "PassengerId",
    "Pclass"
)

df.show(5)
df.printSchema()

fact_passenger.write.mode("overwrite").parquet(f"{gold_path}fact_passenger")

dim_passenger.write.mode("overwrite").parquet(f"{gold_path}dim_passenger")

dim_embarked.write.mode("overwrite").parquet(f"{gold_path}dim_embarked")

dim_class.write.mode("overwrite").parquet(f"{gold_path}dim_class")
