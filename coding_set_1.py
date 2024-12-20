import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, date_sub, to_date, current_date, datediff

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

# Question 1: Employee Status Check
# Create a DataFrame that lists employees with names and their work status. For each employee, determine if they are “Active” or “Inactive” based
# on the last check-in date. If the check-in date is within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive."
# Ensure the first letter of each name is capitalized

#import spark.implicits._

employees = [
("karthik", "2024-12-04"),("neha", "2024-10-20"),("priya", "2024-12-03"),("mohan", "2024-11-02"),("ajay", "2024-09-15"),("vijay", "2024-10-30"),
("veer", "2024-10-25"),("aatish", "2024-10-10"),("animesh", "2024-10-15"),("nishad", "2024-11-01"),("varun", "2024-10-05"),("aadil", "2024-09-30")]

employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])
#employees_df.show()

df1 = employees_df.withColumn("diff", datediff(current_date(), col("last_checkin")))
df1.withColumn("Status", when(col("diff")< 7, "Active").otherwise("Inactive")).show()



