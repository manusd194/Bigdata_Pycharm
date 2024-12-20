import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, avg, date_sub, to_date, current_date, datediff, month, count, lag, year

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"

conf = SparkConf()
conf.set("spark.app.name", "new_combinational")
conf.set("spark.master","Local[*]")

spark = SparkSession.builder  \
    .config(conf=conf) \
    .getOrCreate()


# 1. Calculate the total sales per customer for different order types and apply WHEN to categorize
# customers based on sales amount (High/Medium/Low). Handle nulls for missing sales and join
# with customer info.
customers1 = [(1, "karthik", "2023-01-15"),(2, "Mohan", "2023-02-10"),(3, "Vinay", None)]
customers1col = spark.createDataFrame(customers1,["customer_id","customer_name","date"])

orders1 = [(1, "electronics", 300.50, "2023-01-20"),(1, "clothing", None, "2023-01-25"),(2, "groceries", 120.00, "2023-02-15"),
(3, "clothing", 50.00, "2023-02-20")]
orders1col = spark.createDataFrame(orders1, ["customer_id", "product", "amount", "date"])

# 2. Perform a LEFT JOIN on two DataFrames (employees and salaries), group by department, and
# calculate average salary. Apply WHEN condition to determine salary categories and handle null
# values with COALESCE.
employees2 = [(1, "John", "Sales"),(2, "Jane", "HR"),(3, "Mark", "Finance"),(4, "Emily", "HR")]
employees2col = spark.createDataFrame(employees2, ["employee_id", "employee_name", "department"])

salaries2 = [(1, 5000.00, "2024-01-10"),(2, 6000.00, "2024-01-15"),(4, 7000.00, "2024-01-20")]
salaries2col = spark.createDataFrame(salaries2, ["employee_id", "salary", "date"])

# 3. Read product data from a CSV file, calculate the sales per region using window aggregation, and
# handle missing dates with the latest available data. Write the result in ORC format.
products3 =[(1, "TV", "East", 200),(2, "Laptop", "West", None),(3, "Phone", "North", 150),(4, "Tablet", "East", 300)]
products3col = spark.createDataFrame(products3, ["product_id", "product", "region", "rate"])

regions3 = [(1, "East", "2023-05-01"),(2, "West", None),(3, "North", "2023-05-10")]
regions3col = spark.createDataFrame(regions3, ["product_id","region","date"])

# 4. Perform a FULL OUTER JOIN between two DataFrames (departments and budget), fill missing
# values in budget, group by department, and calculate the total budget using SUM. Use DATE_ADD
# to forecast future budgets.
departments4 =[(101, "HR"),(102, "IT"),(103, "Finance"),(104, "Marketing")]

budget4 =[(101, 50000, "2024-05-01"),(103, 75000, "2024-06-01"),(None, 60000, "2024-06-15")]

# 5. Read data from JSON files, join two DataFrames (customers and transactions), group by month
# using MONTH() function, and calculate the total transaction amount per month. Handle null
# transaction values with default values and write the output to Parquet format.
customers5 = [(1, "karthik", "Premium"),(2, "ajay", "Standard"),(3, "vijay", "Premium"),(4, "vinay", "Standard")]

transactions5 =[(1, "2023-06-01", 100.0),(2, "2023-06-15", 200.0),(1, "2023-07-01", None),(3, "2023-07-10", 50.0)]

# 6. Implement a JOIN between sales and inventory data, group by product category, and calculate
# stock availability per region using window functions. Apply WHEN and OTHERWISE to categorize
# products as "Available" or "Out of Stock".
sales6 = [("TV", "East", 500),("Laptop", "West", 100),("Phone", "North", None),("Tablet", "East", 150)]

inventory6 = [("TV", "East", 700),("Laptop", "West", 50),("Phone", "North", 300),("Tablet", "East", None)]

# 7. Merge daily user engagement data from multiple JSON files, group by user and week using
# WEEK() function, calculate average engagement time, and handle nulls for missing days. Write the
# result to Avro format.
engagement7 = [(1, "2024-01-01", 30),(1, "2024-01-03", None),(2, "2024-01-02", 45),(3, "2024-01-04", 20),(1, "2024-01-05", 60)]

users7 = [(1, "karthik"),(2, "ajay"),(3, "vijay")]

# 8. Perform an INNER JOIN between sales and promotions data, group by month, and calculate total
# sales per promotion type. Handle null values in sales using COALESCE and use DATE_TRUNC to
# aggregate sales by month. Write the result to CSV format.
sales8 = [(1, "2024-05-01", 300),(1, "2024-05-15", None),(2, "2024-06-10", 200),(3, "2024-06-20", 150)]

promotions8 =[(1, "Discount"),(2, "Buy One Get One"),(3, "Cashback")]

# 9. Read multiple file formats (CSV, JSON, and Avro), join the dataframes on product ID, and
# calculate total sales across different formats. Use window aggregation to calculate running totals
# and handle nulls in sales data.
productDetails9 = [(1, "TV", "Electronics"),(2, "Laptop", "Electronics"),(3, "Phone", "Electronics")]

sales9 = [(1, "2023-07-01", 500),(2, "2023-07-15", None),(3, "2023-08-01", 300),(1, "2023-08-10", 200)]

# 10. Calculate employee attendance rates by reading data from CSV files, group by month, and
# apply window functions to rank employees based on attendance percentage. Use WHEN and
# OTHERWISE to classify attendance as "Good", "Average", and "Poor".
attendance10 = [(1, "John", "2023-07", 20),(2, "Jane", "2023-07", None),(3, "Mark", "2023-08", 25),(1, "John", "2023-08", 15)]

employees10 =[(1, "John"),(2, "Jane"),(3, "Mark")]

# 11. Perform an INNER JOIN between a sales DataFrame and a region DataFrame. Use groupBy to
# aggregate the total sales for each region, and apply a window aggregation to rank the regions by
# sales. Use WHEN and OTHERWISE to label regions as "Top" or "Low" based on sales performance,
# handle null values in sales with COALESCE, and apply DATE_FORMAT to transform date columns.
sales11 = [(1, "East", 500, "2023-01-05"),(2, "West", 1000, "2023-02-10"),(3, "North", None, "2023-03-15"),(4, "East", 200, "2023-04-01")]

regions11 = [("East", "Region1"),("West", "Region2"),("North", "Region3"),("South", "Region4")]

# 12. Join a customer transactions DataFrame with a discount DataFrame. Group by customer and
# month, then calculate the total discount applied. Use WHEN and OTHERWISE to categorize the
# discount levels as "Low", "Medium", or "High". Handle null discounts with a default value and
# calculate the maximum discount using a window function. Write the result to Parquet format.
transactions12 =[(1, "2023-06-01", 100.0, null),(2, "2023-06-15", 200.0, 10.0),(1, "2023-07-01", 300.0, 15.0),(3, "2023-07-10", 50.0, 5.0)]

discounts12 =[(1, "Summer Sale", 10.0),(2, "Winter Sale", 15.0),(3, "Spring Sale", None)]

# 13. Read product sales data from multiple formats (CSV, JSON, and Parquet), then join the data on
# product ID. Group by product category and calculate total sales. Use WHEN and OTHERWISE to
# apply dynamic pricing based on the total sales. Implement a window function to compute the
# running total for each category. Write the result to ORC format.
sales13 =[(1, "TV", "Electronics", 500.0, "2023-01-15"),(2, "Phone", "Electronics", None, "2023-02-10"),(3, "Laptop", "Computers", 300.0, "2023-03-05"),
(4, "TV", "Electronics", 400.0, "2023-04-01")]

categories13 =[("TV", "Electronics"),("Phone", "Electronics"),("Laptop", "Computers"),("Tablet", "Electronics")]

# 14. Perform a FULL OUTER JOIN between customer data and transaction data. Group by customer
# and year using the YEAR function and calculate the total transactions per customer. Use WHEN to
# classify customers as "Frequent" or "Occasional" based on the number of transactions. Handle
# missing transaction amounts using COALESCE. Write the result to Avro format.

customers14 = [(1, "Ajay", "2023-01-15"),(2, "Bhim", "2023-02-10"),(3, "Carni", None)]

transactions14 =[(1, "2023-01-20", 300.50),(1, "2023-01-25", None),(2, "2023-02-15", 120.00),(3, "2023-02-20", 50.00)]

# 15. Read product and sales data from CSV and JSON files, then perform a LEFT JOIN on product ID.
# Group by product and calculate the total sales per product category. Use WHEN and OTHERWISE to
# set promotional discounts for products based on total sales. Apply a window function to rank
# products within each category based on total sales. Handle null sales values with COALESCE, and
# write the final result to Parquet format.
products15 =[(1, "TV", "Electronics"),(2, "Phone", "Electronics"),(3, "Laptop", "Computers"),(4, "Tablet", "Electronics")]
sales15 =[(1, "2023-06-01", 500.0),(2, "2023-06-15", None),(3, "2023-07-01", 300.0),(4, "2023-07-10", 50.0)]

# 16. Join an employee DataFrame with a department budget DataFrame. Use groupBy to calculate
# the total budget allocated to each department. Use WHEN and OTHERWISE to flag departments
# that are over or under budget. Apply a window function to compute the cumulative budget per
# department. Handle null values in the budget with default values and write the result to ORC format.
employees16 =[(1, "John", "HR"),(2, "Jane", "IT"),(3, "Mark", "Finance"),(4, "Emily", "HR")]

budget16 =[("HR", 50000, "2023-01-10"),("IT", None, "2023-01-15"),("Finance", 75000, "2023-01-20")]

# 17. Perform a LEFT OUTER JOIN between sales and customer data. Use groupBy to calculate the
# total number of transactions for each customer. Use WHEN and OTHERWISE to apply loyalty
# discounts based on the total number of transactions. Apply a window function to compute the
# running total of transactions for each customer and handle null transaction counts with COALESCE.
# Write the output to JSON format.
sales17 =[(1, "2023-05-01", 500.0),(2, "2023-05-15", 200.0),(1, "2023-06-01", None),(3, "2023-06-10", 300.0)]

customers17 =[(1, "Alia"),(2, "Bhim"),(3, "Carnio")]

# 18. Read order and product data from different file formats (CSV and Avro), then perform a FULL
# OUTER JOIN on product ID. Use groupBy to calculate the total orders per product category. Use
# WHEN and OTHERWISE to dynamically assign stock levels based on the total orders. Apply window
# functions to rank products based on total orders, handle missing order counts with COALESCE, and
# write the result to Avro format.
orders18 =[(1, "2023-01-05", 100, "Electronics"),(2, "2023-01-10", 200, "Clothing"),(3, "2023-02-15", None, "Electronics"),
(4, "2023-03-01", 150, "Electronics")]

products18 =[(1, "TV", "Electronics"),(2, "Laptop", "Computers"),(3, "Phone", "Electronics")]

# 19. Join a subscription DataFrame with a payment DataFrame. Group by customer and subscription
# type, and calculate the total amount paid by each customer. Use WHEN and OTHERWISE to apply
# late fees based on payment delays. Apply a window function to compute the total amount paid by
# subscription type and handle null payment amounts using COALESCE. Write the result to Parquet format.
subscriptions19 =[(1, "Alia", "Premium", "2023-01-15"),(2, "Bhim", "Standard", "2023-02-10"),(3, "Carnio", "Premium", "2023-03-05")]

payments19 =[(1, "2023-01-20", 50.0),(1, "2023-02-01", None),(2, "2023-02-15", 20.0),(3, "2023-03-10", 50.0)]

# 20. Perform a RIGHT JOIN between an inventory DataFrame and a warehouse DataFrame. Group
# by product and warehouse location, and calculate the total inventory for each product. Use WHEN
# and OTHERWISE to mark warehouses as "Overstocked" or "Understocked" based on total
# inventory. Apply a window function to rank products by stock levels and handle missing inventory
# values with COALESCE. Write the result to ORC format.
inventory20 =[(1, "TV", "East", 500),(2, "Laptop", "West", None),(3, "Phone", "North", 300)]

warehouses20 =[("East", "Region1"),("West", "Region2"),("North", "Region3")]

# 21. Read sales data from a CSV file and employee data from a JSON file. Perform an INNER JOIN on
# employee ID, group by employee department, and calculate the total sales per department. Use
# WHEN and OTHERWISE to categorize departments into "High Performing" or "Low Performing"
# based on sales. Handle missing sales data with a default value. Apply a window function to rank
# employees based on their individual sales and write the output to Avro format.
sales21 =[(1, 500.0, "2023-03-01"),(2, 300.0, "2023-04-10"),(3, None, "2023-05-15"),(4, 700.0, "2023-06-01")]

employees21 =[(1, "John", "Sales"),(2, "Jane", "Marketing"),(3, "Sam", "Sales"),(4, "Paul", "Finance")]

# 22. Perform a LEFT OUTER JOIN between customer subscription data and their payment records.
# Group by customer and subscription type, calculate the total payments made per subscription. Use
# WHEN and OTHERWISE to apply subscription discounts based on total payments. Handle null
# payment values with COALESCE, and apply a window aggregation to compute the running total for
# each customer’s subscription payments. Write the result to ORC format.
subscriptions22 =[(1, "Alia", "Gold", "2023-01-05"),(2, "Bhim", "Silver", "2023-02-10"),(3, "Canilo", "Gold", "2023-03-15"),
(4, "David", "Bronze", "2023-04-20")]

payments22 =[(1, "2023-01-10", 150.0),(2, "2023-02-12", None),(3, "2023-03-20", 200.0),(4, "2023-04-25", 50.0)]

# 23. Read transaction data from CSV and JSON, then perform a FULL OUTER JOIN with the customer
# demographic data. Group by region and customer age group, and calculate total sales. Use WHEN
# and OTHERWISE to label age groups as "Young", "Middle-aged", or "Senior" based on their sales
# patterns. Apply a window function to rank regions by sales volume, and handle null values in
# transaction amounts with COALESCE. Write the result to Parquet format.
transactions23 =[(1, "2023-01-05", 500.0, "East", 25),(2, "2023-02-10", None, "West", 40),(3, "2023-03-15", 300.0, "North", 60),
(4, "2023-04-20", 700.0, "South", 35)]

customers23 =[(1, "Ajay", "East", 25),(2, "Bhim", "West", 40),(3, "Canilo", "North", 60),(4, "David", "South", 35)]

# 24. Read employee timesheet data from CSV and project data from Parquet. Perform a RIGHT
# OUTER JOIN to calculate total hours worked per project. Use WHEN and OTHERWISE to assign
# projects into categories like "Overworked" or "Balanced" based on hours worked. Apply a window
# function to calculate the cumulative hours worked by each employee and handle missing time
# entries with COALESCE. Write the result to ORC format.
timesheets24 =[(1, "ProjectA", 40, "2023-05-01"),(2, "ProjectB", 35, "2023-05-08"),(3, "ProjectA", None, "2023-05-15"),(4, "ProjectC", 50, "2023-06-01")]

projects24 =[("ProjectA", "HighPriority"),("ProjectB", "LowPriority"),("ProjectC", "MediumPriority")]

# 25. Perform a JOIN between order details and inventory data, using a window function to rank
# orders by quantity for each product. Group by product and calculate the total quantity ordered.
# Use WHEN and OTHERWISE to set inventory levels based on product demand, handling null order
# quantities with default values. Write the results to JSON format.
orders25 =[(1, "TV", 5, "2023-03-01"),(2, "Laptop", None, "2023-04-10"),(3, "Phone", 8, "2023-05-15"),(4, "Tablet", 3, "2023-06-01")]
inventory25 =[("TV", 100),("Laptop", 50),("Phone", 200),("Tablet", 75)]

# 26. Read multiple product reviews from CSV, JSON, and Parquet. Perform a LEFT OUTER JOIN
# between product details and customer reviews. Group by product category and customer age
# group to calculate average ratings. Use WHEN and OTHERWISE to classify products as "Highly
# Rated", "Moderately Rated", or "Poorly Rated" based on average rating. Apply a window function
# to rank products within each category. Handle null values in review ratings with default values and
# write the result to ORC format.
reviews26 =[(1, "Electronics", 5, "2023-03-10", 25),(2, "Clothing", None, "2023-04-15", 40),(3, "Electronics", 4, "2023-05-20", 30),
(4, "Home Appliances", 3, "2023-06-25", 35)]

products26 =[(1, "TV", "Electronics"),(2, "Shirt", "Clothing"),(3, "Fridge", "Home Appliances"),(4, "Phone", "Electronics")]

# 27. Perform a JOIN between rental data and customer details. Group by rental location and
# customer age, then calculate the total rentals per location. Use WHEN and OTHERWISE to assign
# loyalty points based on the number of rentals. Handle null rental amounts with COALESCE, and
# apply a window function to compute the cumulative rentals per location. Write the output to Avro format.
rentals27 =[(1, "New York", 3, "2023-07-01", 30),(2, "Los Angeles", None, "2023-07-10", 45),(3, "Chicago", 5, "2023-07-20", 25),(4, "Houston", 7, "2023-08-01", 40)]

customers27 =[(1, "John", "New York", 30),(2, "Jane", "Los Angeles", 45),(3, "Paul", "Chicago", 25),(4, "Emily", "Houston", 40)]

# 28. Read customer feedback from Parquet and product data from CSV. Perform an INNER JOIN on
# product ID, group by product category and region, and calculate the average feedback rating per
# category. Use WHEN and OTHERWISE to classify regions as "Positive" or "Negative" based on
# feedback scores. Apply a window function to rank regions by average feedback and handle null
# ratings with COALESCE. Write the output to JSON format
feedback28 =[(1, "TV", "East", 5, "2023-08-01"),(2, "Phone", "West", None, "2023-08-15"),(3, "Laptop", "North", 4, "2023-09-01")
(4, "Tablet", "South", 3, "2023-09-15")]

products28 =[(1, "TV", "Electronics"),(2, "Phone", "Electronics"),(3, "Laptop", "Computers"),(4, "Tablet", "Electronics")]

# 29. Join vehicle maintenance records with service center data. Group by vehicle type and calculate
# the total maintenance cost. Use WHEN and OTHERWISE to classify service centers as "High Cost" or
# "Low Cost" based on average maintenance costs. Apply a window function to rank service centers
# within each region, handle missing cost values using default values, and write the result to ORC format.
maintenance29 =[(1, "Car", 500, "2023-03-01", "East"),(2, "Truck", None, "2023-04-10", "West"),(3, "Bike", 300, "2023-05-15", "North"),
(4, "Car", 700, "2023-06-01", "South")]

serviceCenters29 =[("East", "ServiceA"),("West", "ServiceB"),("North", "ServiceC"),("South", "ServiceD")]

# 30. Read order details from a CSV file and product data from Parquet. Perform a LEFT OUTER JOIN
# on product ID and validate the order amounts. Orders with negative or zero values should be
# marked as "Invalid" using WHEN and OTHERWISE. Group by product category and calculate the
# total valid sales. Apply a window function to rank products by sales in each category, handling null
# values in order amounts. Write the result to JSON format.
orders30 =[(1, "Laptop", -1000.0, "2023-03-01"),(2, "TV", 1500.0, "2023-04-10"),(3, "Phone", None, "2023-05-15"),
(4, "Tablet", 700.0, "2023-06-01")]

products30 =[(1, "Laptop", "Electronics"),(2, "TV", "Electronics"),(3, "Phone", "Electronics"),(4, "Tablet", "Electronics")]

# 31. Read sales data from CSV and employee data from JSON. Perform an INNER JOIN on employee
# ID, and validate the sales values, marking entries with null or missing values as "Incomplete".
# Group by department and calculate the total sales for each. Apply a window function to rank
# employees within each department by sales, and handle null sales values using default values.
# Write the result to ORC format.
sales31 =[(1, 1000.0, "2023-01-05"),(2, 2000.0, "2023-02-10"),(3, None, "2023-03-15"),(4, 500.0, "2023-04-20")]

employees31 =[(1, "John", "Sales"),(2, "Jane", "Marketing"),(3, "Sam", "Sales"),(4, "Paul", "Finance")]

# 32. Join customer orders and their payment details. Validate that the order date is before the
# payment date, and mark any invalid entries using WHEN and OTHERWISE. Group by customer and
# calculate the total valid payments. Use window aggregation to compute the running total of valid
# payments per customer and handle null payment amounts with default values. Write the result to
# Parquet format.
orders32 =[(1, "2023-05-01", 500.0),(2, "2023-06-10", 300.0),(3, "2023-07-15", 700.0),(4, "2023-08-01", None)]

payments32 =[(1, "2023-05-05", 500.0),(2, "2023-06-15", None),(3, "2023-07-20", 700.0),(4, "2023-08-05", 400.0)]

# 33. Read employee attendance and payroll data from CSV. Validate that working hours are greater
# than zero and handle invalid entries using WHEN and OTHERWISE. Group by employee department
# and calculate the total valid working hours. Apply a window function to rank employees by
# attendance and handle missing attendance values with default entries. Write the result to Avro format.
attendance33 =[(1, "2023-05-01", 8),(2, "2023-05-02", -2),(3, "2023-05-03", 6),(4, "2023-05-04", None)]

payroll33 =[(1, "John", "HR"),(2, "Jane", "Finance"),(3, "Sam", "IT"),(4, "Paul", "Marketing")]

# 34. Perform a FULL OUTER JOIN between inventory and sales data, and validate that sales
# quantities do not exceed available inventory using WHEN and OTHERWISE. Group by product
# category and calculate the total remaining inventory. Apply window aggregation to rank products
# by remaining inventory within each category, handling null values in sales quantities with
# COALESCE. Write the result to ORC format.
inventory34 =[(1, "Laptop", 100),(2, "TV", 50),(3, "Phone", 200),(4, "Tablet", None)]

sales34 =[(1, "Laptop", 20, "2023-03-01"),(2, "TV", 60, "2023-04-10"),(3, "Phone", 100, "2023-05-15"),(4, "Tablet", 50, "2023-06-01")]

# 35. Join product sales and customer reviews, and validate that review ratings are within a valid
# range (1-5). Group by product category and calculate the average valid review score. Use WHEN
# and OTHERWISE to classify products as "Top Rated" or "Low Rated". Apply a window function to
# rank products within each category by average review score, handling missing review ratings.
# Write the result to JSON format.
sales35 =[(1, "Laptop", 500.0, "2023-05-01"),(2, "TV", 300.0, "2023-06-10"),(3, "Phone", 700.0, "2023-07-15"),(4, "Tablet", 400.0, "2023-08-01")]
reviews35 =[(1, "Laptop", 5),(2, "TV", None),(3, "Phone", 4),(4, "Tablet", 6)]

# 36. Join student attendance data and exam results. Validate that students with less than 75%
# attendance should be marked as "Not Eligible" for exams. Group by class and calculate the average
# exam score for eligible students. Use window aggregation to rank students by exam score within
# each class and handle null exam scores. Write the result to Parquet format.
attendance36 =[(1, "John", 80, "2023-01-01"),(2, "Jane", 70, "2023-01-01"),(3, "Sam", 85, "2023-01-01"),(4, "Paul", 60, "2023-01-01")]

results36 =[(1, "John", 90),(2, "Jane", 85),(3, "Sam", None),(4, "Paul", 70)]

# 37. Read product details and sales data. Validate that product prices are positive, marking invalid
# entries with WHEN and OTHERWISE. Group by product category and calculate the total valid sales
# revenue. Apply window aggregation to rank products within each category by sales revenue,
# handling null prices. Write the result to Avro format.
products37 =[(1, "Laptop", 1000.0, "Electronics"),(2, "TV", -500.0, "Electronics"),(3, "Phone", 700.0, "Electronics"),(4, "Tablet", None, "Electronics")]

sales37 =[(1, "Laptop", 10),(2, "TV", 20),(3, "Phone", 15),(4, "Tablet", 5)]

# 38. Perform a JOIN between customer orders and their feedback. Validate that feedback ratings
# are submitted only for orders that have been delivered (based on date comparison). Group by
# customer and calculate the average feedback score. Use window aggregation to rank customers by
# feedback score, handling null feedback values. Write the result to ORC format.
orders38 =[(1, "2023-03-01", "Delivered"),(2, "2023-04-10", "Pending"),(3, "2023-05-15", "Delivered"),(4, "2023-06-01", "Delivered")]

feedback38 =[(1, "2023-03-05", 5),(2, "2023-04-15", None),(3, "2023-05-20", 4),(4, "2023-06-05", 3)]
