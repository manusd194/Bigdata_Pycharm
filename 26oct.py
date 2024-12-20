from pyspark.sql import SparkSession
from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"

sc = SparkContext("local[*]", "sparkrdd")

ls = [10,20,30,40,50,60,70,80,91]
rdd1 = sc.parallelize(ls)

# print("using take")

# ds1 = rdd1.take(rdd1.count())
# for i in ds1:
#     print(i)
#
# ds = rdd1.collect()
# print("using collect")
# for i in ds:
#     print(i)
#
# avg = rdd1.mean()
# print(avg)
#
# sum = rdd1.reduce(lambda acc, x: acc + x)
# print(sum)
# avg1 = sum/rdd1.count()
# print(avg1)
# print(type(rdd1))
#rdd1.saveAstextFile("C:/Users/manu/Desktop/spark_p/pyspark")

fruit = [(1, "apple"), (2, "banana"), (3, "carrot"), (4, "doritos")]
color = [(1, "red"), (2, "yellow"), (3, "pink"), (4, "brown")]

rdd2 = sc.parallelize(fruit)
rdd3 = sc.parallelize(color)

a = rdd2.join(rdd3)  # rdd of tuple as op

ds2 = a.take(a.count())  # a list of tuple as op
for i in ds2:
    print(i)


