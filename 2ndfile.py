from pyspark.sql import SparkSession
from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"

sc = SparkContext("local[4]", "sparkrdd")
# rdd = sc.parallelize([1, 2, 3, 2, 1, 4, 5])
# distinct_rdd = rdd.distinct()
# for item in distinct_rdd.collect():
#  print(item)

rdd1 = sc.textFile("C:/Users/manu/Desktop/spark_p/data.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
rdd5 = rdd4.sortBy(lambda x: x[1], False)

for i in rdd5.collect():
    print(i)
