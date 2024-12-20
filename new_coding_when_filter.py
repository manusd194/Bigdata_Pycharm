import os

import null

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, avg, date_sub, to_date, current_date, datediff, month, count, lag, year

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"

conf = SparkConf()
conf.set("spark.app.name", "new_coding_when_filter")
conf.set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

# 1. Employee Performance Analysis
# You have a DataFrame employee_performance with columns: employee_id, department, performance_score, review_date, and position.
#  Create a new column review_month using month extracted from review_date.
#  Filter records where the position ends with 'Manager' and the performance_score is greater than 80.
#  Group by department and review_month, and calculate:
# o The average performance_score per department per month.
# o The count of employees who received a performance_score above 90.
#  Use the lag function to calculate the performance improvement or decline for each employee compared to their previous review.
# Sample Data:
employees = [("E001", "Sales", 85, "2024-02-10", "Sales Manager"),("E002", "HR", 78, "2024-03-15", "HR Assistant"),("E003", "IT", 92, "2024-01-22", "IT Manager"),
             ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),("E005", "HR", 95, "2024-03-20", "HR Manager")]

employees_df = spark.createDataFrame(employees,["employee_id","department", "performance_score", "review_date", "position"])
#employees_df.show()
employees_df1 = employees_df.filter(col("position").contains("Manager")).filter(col("performance_score") > 80).withColumn("review_month", month(col("review_date")))
#employees_df1.withColumn("review_month", month(col("review_date"))).groupby(col("department"), col("review_month")).agg(avg(col("performance_score"))).show()

#employees_df1.filter(col("performance_score") > 90).groupby(col("department"), col("review_month")).agg(count("performance_score")).show()

employees_df_window = Window.partitionBy(col("department"), col("review_date")).orderBy(col("performance_score"))
#employees_df.withColumn("lag", lag(col("performance_score")).over(employees_df_window) - col("performance_score")).show()

# 2. Customer Churn Analysis
# You have a DataFrame customer_churn with columns: customer_id, subscription_type, churn_status,churn_date, revenue, and country.
#  Create a new column churn_year using year extracted from churn_date.
#  Filter records where subscription_type starts with 'Premium' and churn_status is not null.
#  Group by country and churn_year, and calculate:
# o The total revenue lost due to churn per year.
# o The average revenue per churned customer.
# o The count of churned customers.
#  Use the lead function to find the next year's revenue trend for each country.
# Sample Data:
customer_2 = [("C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
              ("C002", "Basic", "No", None, 400, "Canada"),
              ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
              ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
              ("C005", "Basic", "No", None, 300, "India")]

columns =("customer_id string, subscription_type string, churn_status string, churn_date string, revenue int, country string")
customer_2_df = spark.createDataFrame(customer_2, columns)
#customer_2_df.show()
#customer_2_df.filter(col("subscription_type").startswith("Premium")).filter(col("churn_status").isNotNull()).show()

customer_2_year = customer_2_df.withColumn("churn_year", year(col("churn_date")))
#customer_2_year_2=customer_2_year.filter(col("revenue").cast("int"))
#customer_2_year_2.show()
#customer_2_year_2.groupby(col("country"), col("churn_year")).agg(avg(col("revenue")), count(col("revenue"))).show()
#customer_2_year_2.groupby(col("country"), col("churn_year")).agg(sum(col("revenue"))).show()

#customer_2_year.printSchema()

#customer_2_year.groupby(col("country")).agg(count(col("revenue")), avg(col("revenue")), sum(col("revenue")), min(col("revenue")), max(col("revenue"))).show()


# 3. Sales Target Achievement Analysis
# You have a DataFrame sales_targets with columns: salesperson_id, sales_amount, target_amount, sale_date, region, and product_category.
#  Create a new column target_achieved based on whether sales_amount is greater than or equal to target_amount.
#  Filter records where product_category starts with 'Electronics' and ends with 'Accessories'.
#  Group by region and product_category, and calculate:
# o The total sales_amount.
# o The minimum sales_amount for any salesperson.
# o The number of salespersons who achieved their targets.
#  Use the lag function to compare each salesperson's current sales with their previous sales.
# Sample Data:

sales_target = [("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),("S002", 8000, 9000, "2023-12-11", "South", "Home Appliances"),
                ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),("S004", 10000, 15000, "2023-12-13", "West", "Electronics Accessories"),
                ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories")]

sales_target_col = spark.createDataFrame(sales_target, ["salesperson_id", "sales_amount", "target_amount", "sale_date", "region", "product_category"])
sales_target_col_1 = sales_target_col.withColumn("target_achieved", when(col("sales_amount") >= col("target_amount"), "Achieved").otherwise("Not Achieved"))
#sales_target_col_1.filter(col("target_achieved") == "Achieved").groupby(col("region"), col("product_category")).agg(count(col("target_achieved"))).show()

# 19. Student Performance Analysis
# You have a DataFrame student_performance with columns: student_id, subject, exam_date, score, grade, and school.
#  Create a new column exam_month using month extracted from exam_date.
#  Filter records where score is below 50 and grade is 'F'.
#  Group by school and exam_month, and calculate:
# o The average score per school.
# o The highest score in each subject.
# o The count of failing grades.
#  Use the lag function to analyze score improvement or decline for each student over time.
# Sample Data:
qs19 = [("S001", "Mathematics", "2023-10-01", 45, "F", "School A"), ("S002", "Science", "2023-11-05", 55, "D", "School B"),
        ("S003", "History", "2023-12-10", 35, "F", "School A"), ("S004", "Mathematics", "2023-12-15", 80, "B", "School C"),
        ("S005", "English", "2024-01-01", 65, "C", "School B")]
col_q19 = spark.createDataFrame(qs19, ["student_id", "subject", "exam_date", "score", "grade", "school"])

col_q19_1 = col_q19.withColumn("exam_month", month(col("exam_date")))
#col_q19_1.show()
#col_q19_1.filter((col("grade") == "F") & (col("score") < 50)).show()
#col_q19_1.groupby(col("school"), col("exam_month")).agg(avg(col("score")), count(col("score"))).show()
#col_q19_1.filter(col("grade") == "F").groupby(col("school"), col("exam_month")).agg(count(col("score"))).show()

q19_partition = Window.partitionBy(col("student_id")).orderBy(col("exam_month"))
#col_q19_1.withColumn("score", (lag("score").over(q19_partition) - col("score"))).show()













