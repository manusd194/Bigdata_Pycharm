import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"  # or "python" depending on your Python executable

conf = SparkConf()
conf.set("spark.app.name", "spark-program")
conf.set("spark.master", "local[*]")

# spark = (SparkSession.builder
#          .config(conf)
#          .getOrCreate())


#Create Spark Session
spark = SparkSession.builder \
    .appName("Example") \
    .master("local[*]") \
    .getOrCreate()


schema="id int,Name string,Age int"

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "C:/Users/manu/Documents/info.csv") \
    .load()

df.show()
df.printSchema()