import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, avg, date_sub, to_date, current_date, datediff, month, count, lag, year

os.environ["PYSPARK_PYTHON"] = "C:/Users/manu/Documents/Python/Python37/python.exe"

conf = SparkConf()
conf.set("spark.app.name", "new_join_coding")
conf.set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

# 1. Employee Department Join
# Problem: You have two DataFrames: employees and departments. Join them to find employees along with their department names.
employees = [("E001", "Alice", "D001"),("E002", "Bob", "D002"),("E003", "Charlie", "D001"),("E004", "David", "D003"),("E005", "Eve", "D002"),
("E006", "Frank", "D001"),("E007", "Grace", "D004")]
employees_col = spark.createDataFrame(employees,["employee_id", "employee_name", "department_id"])

departments = [("D001", "HR"),("D002", "Finance"),("D003", "IT"),("D004", "Marketing"),("D005", "Sales")]
departments_col = spark.createDataFrame(departments,["department_id", "department_name"])

# employees_col.join(departments_col, employees_col.department_id == departments_col.department_id, "inner") \
#     .select(employees_col.employee_name, departments_col.department_name).show()


# 2. Product Sales Analysis
# Problem: You have two DataFrames: products and sales. Join them to find the total sales amount for each product.
products = [("P001", "Laptop"),("P002", "Mobile"),("P003", "Tablet"),("P004", "Monitor"),("P005", "Keyboard")]
products_col = spark.createDataFrame(products,["product_id", "product_name"])

sales = [("S001", "P001", 500.0),("S002", "P002", 300.0),("S003", "P001", 700.0),("S004", "P003", 200.0),("S005", "P002", 400.0),
("S006", "P004", 600.0),("S007", "P005", 150.0)]
sales_col = spark.createDataFrame(sales,["sale_id", "product_id", "amount"])

#products_col.join(sales_col, products_col.product_id == sales_col.product_id , "inner").groupby(col("product_name")).agg((col("amount")))


# 3. Student Course Enrollment
# Problem:You have two DataFrames: students and courses. Join them to find the courses each student is enrolled in.
students = [("S001", "John"),("S002", "Emma"),("S003", "Olivia"),("S004", "Liam"),("S005", "Noah")]
students_col = spark.createDataFrame(students,["student_id", "student_name"])

courses = [("C001", "Math"),("C002", "Science"),("C003", "History"),("C004", "Art")]
courses_col = spark.createDataFrame(courses,["course_id", "course_name"])

enrollments = [("E001", "S001", "C001"),("E002", "S002", "C002"),("E003", "S001", "C003"),("E004", "S003", "C001"),("E005", "S004", "C004"),
("E006", "S005", "C002"),("E007", "S005", "C003")]
enrollments_col = spark.createDataFrame(enrollments,["enrollment_id", "student_id", "course_id"])

# enrollments_col.join(students_col, ["student_id"]) \
#     .join(courses_col, courses_col.course_id == courses_col.course_id) \
#     .select(students_col.student_name, courses_col.course_name) \
# .orderBy(col("student_name")).show()


# 4. Customer Order Details
# Problem:You have three DataFrames: customers, orders, and products. Join them to find the order details for each customer.
customers = [("C001", "Alice"),("C002", "Bob"),("C003", "Charlie"),("C004", "David")]
customers_col = spark.createDataFrame(customers, ["customer_id", "customer_name"])

orders = [("O001", "C001", "P001"),("O002", "C002", "P002"),("O003", "C003", "P003"),("O004", "C001", "P004"),("O005", "C004", "P001")]
orders_col = spark.createDataFrame(orders,["order_id", "customer_id", "product_id"])

products_q4 = [("P001", "Laptop"),("P002", "Mobile"),("P003", "Tablet"),("P004", "Monitor")]
products_col_q4 = spark.createDataFrame(products,["product_id", "product_name"])

# customers_col.join(orders_col, ["customer_id"]) \
#     .join(products_col_q4,orders_col.product_id == products_col_q4.product_id, "inner") \
#     .orderBy(col("customer_name")).show()


#5. Company Projects and Employees
#You have three DataFrames: projects, employees, and assignments. Join them to find the employees assigned to each project.
projects = [("PR001", "Project Alpha"),("PR002", "Project Beta"),("PR003", "Project Gamma")]
projects_col_q5 = spark.createDataFrame(projects,["project_id", "project_name"])

employees_q5 = [("E001", "Alice"),("E002", "Bob"),("E003", "Charlie"),("E004", "David")]
employees_q5_col = spark.createDataFrame(employees_q5,["employee_id", "employee_name"])

assignments_q5 = [("A001", "PR001", "E001"),("A002", "PR001", "E002"),("A003", "PR002", "E001"),("A004", "PR003", "E003"),("A005", "PR003", "E004")]
assignments_q5_col = spark.createDataFrame(assignments_q5,["assignment_id", "project_id", "employee_id"])

# projects_col_q5.join(assignments_q5_col, ["project_id"]) \
#     .join(employees_q5_col, assignments_q5_col.employee_id == employees_q5_col.employee_id) \
#     .select(employees_q5_col.employee_name, projects_col_q5.project_id, projects_col_q5.project_name).show()


# 6. Hospital Patient Visits
# You have two DataFrames: patients and visits. Join them to find the visit details for each patient.
patients_q6 = [("P001", "Alice", 30),("P002", "Bob", 40),("P003", "Charlie", 25)]
patients_q6_col = spark.createDataFrame(patients_q6,["patient_id", "patient_name", "age"])

visits_q6 = [("V001", "P001", "2024-01-01", "Routine Checkup"),("V002", "P002", "2024-01-05", "Consultation"),("V003", "P001", "2024-01-10", "Follow-up"),
("V004", "P003", "2024-01-12", "Emergency"),("V005", "P001", "2024-01-15", "Routine Checkup")]
visits_q6_col = spark.createDataFrame(visits_q6,["visit_id", "patient_id", "visit_date", "visit_reason"])

# patients_q6_col.join(visits_q6_col, ["patient_id"], "inner").show()


# 7. Movie Ratings and Users
# Problem:You have three DataFrames: movies, users, and ratings. Join them to find the average rating for each movie.
movies_q7 = [("M001", "Inception"),("M002", "The Dark Knight"),("M003", "Interstellar"),("M004", "Titanic")]
movies_q7_com = spark.createDataFrame(movies_q7,["movie_id", "movie_title"])

users_q7 = [("U001", "Alice"),("U002", "Bob"),("U003", "Charlie")]
users_q7_col = spark.createDataFrame(users_q7,["user_id", "user_name"])

ratings_q7 = [("R001", "M001", "U001", 5),("R002", "M002", "U001", 4),("R003", "M003", "U002", 5),("R004", "M002", "U003", 3),
("R005", "M001", "U002", 4),("R006", "M004", "U001", 2)]
ratings_q7_col = spark.createDataFrame(ratings_q7,["rating_id", "movie_id", "user_id", "rating"])

#movies_q7_com.join(ratings_q7_col, ["movie_id"]).groupby(col("movie_title")).agg(avg(col("rating"))).show()

#8. Book Author Publication
#Problem:You have two DataFrames: books and authors. Join them to find the books written by each author.
books_q8 = [("B001", "Book One", "A001"),("B002", "Book Two", "A002"),("B003", "Book Three", "A001"),("B004", "Book Four", "A003")]
books_q8_col = spark.createDataFrame(books_q8,["book_id", "book_title", "author_id"])

authors_q8 = [("A001", "Author One"),("A002", "Author Two"),("A003", "Author Three")]
authors_q8_col = spark.createDataFrame(authors_q8,["author_id", "author_name"])


#9. Comprehensive Sales Analysis
#Problem:You have four DataFrames: customers, orders, products, and regions. Join them to find the sales amount for each customer along with their region.
customers_q9 = [("C001", "Alice", "R001"),("C002", "Bob", "R002"),("C003", "Charlie", "R003"),("C004", "David", "R001"),("C005", "Eve", "R004")]
customers_q9_col = spark.createDataFrame(customers_q9,["customer_id", "customer_name", "region_id"])

orders_q9 = [("O001", "C001", "P001"),("O002", "C002", "P002"),("O003", "C001", "P003"),("O004", "C003", "P001"),("O005", "C004", "P004"),
("O006", "C005", "P002"),("O007", "C005", "P003")]
orders_q9_col = spark.createDataFrame(orders_q9,["order_id", "customer_id", "product_id"])

products_q9 = [("P001", "Laptop", 800.0),("P002", "Mobile", 400.0),("P003", "Tablet", 300.0),("P004", "Monitor", 200.0)]
products_q9_col = spark.createDataFrame(products_q9,["product_id", "product_name", "price"])

regions_q9 = [("R001", "North"),("R002", "South"),("R003", "East"),("R004", "West")]
regions_q9_col = spark.createDataFrame(regions_q9,["region_id", "region_name"])


#10. Employee Project Allocation
#Problem:You have four DataFrames: employees, projects, allocations, and departments. Join them to find which employees are allocated to which
# projects along with their department names.
employees_q10 = [("E001", "Alice", "D001"),("E002", "Bob", "D002"),("E003", "Charlie", "D001"),("E004", "David", "D003"),("E005", "Eve", "D002")]
employees_q10_col = spark.createDataFrame(employees_q10,["employee_id", "employee_name", "department_id"])

projects_q10 = [("PR001", "Project Alpha"),("PR002", "Project Beta"),("PR003", "Project Gamma")]
projects_q10_col = spark.createDataFrame(projects_q10,["project_id", "project_name"])

allocations_q10 = [("A001", "E001", "PR001"),("A002", "E002", "PR002"),("A003", "E001", "PR003"),("A004", "E003", "PR001"),("A005", "E004", "PR003"),
("A006", "E005", "PR002")]
allocations_q10_col = spark.createDataFrame(allocations_q10,["allocation_id", "employee_id", "project_id"])

departments_q10 = [("D001", "HR"),("D002", "Finance"),("D003", "IT")]
departments_q10_col = spark.createDataFrame(departments_q10,["department_id", "department_name"])


#11. Patient Visit Details
#Problem:You have four DataFrames: patients, visits, doctors, and hospitals. Join them to find out which patients visited which doctors in which
# hospitals.
patients_q11 = [("P001", "Alice"),("P002", "Bob"),("P003", "Charlie"),("P004", "David")]
patients_q11_col = spark.createDataFrame(patients_q11,["patient_id", "patient_name"])

visits_q11 = [("V001", "P001", "D001", "H001"),("V002", "P002", "D002", "H002"),("V003", "P003", "D001", "H003"),("V004", "P001", "D003", "H001"),
("V005", "P004", "D002", "H002")]
visits_q11_col = spark.createDataFrame(visits_q11,["visit_id", "patient_id", "doctor_id", "hospital_id"])

doctors_q11 = [("D001", "Dr. Smith"),("D002", "Dr. Johnson"),("D003", "Dr. Lee")]
doctors_q11_col = spark.createDataFrame(doctors_q11,["doctor_id", "doctor_name"])

hospitals_q11 = [("H001", "City Hospital"),("H002", "County Hospital"),("H003", "General Hospital")]
hospitals_q11_col = spark.createDataFrame(hospitals_q11,["hospital_id", "hospital_name"])


#12. Course Enrollment and Instructors
#Problem:You have four DataFrames: students, courses, instructors, and enrollments. Join them to find the instructors for each course and the
# students enrolled.
students_q12 = [("S001", "John"),("S002", "Emma"),("S003", "Olivia"),("S004", "Liam")]
students_q12_col = spark.createDataFrame(students_q12,["student_id", "student_name"])

courses_q12 = [("C001", "Math", "I001"),("C002", "Science", "I002"),("C003", "History", "I001"),("C004", "Art", "I003")]
courses_q12_col = spark.createDataFrame(courses_q12,["course_id", "course_name", "instructor_id"])

instructors_q12 = [("I001", "Prof. Brown"),("I002", "Prof. Green"),("I003", "Prof. White")]
instructors_q12_col = spark.createDataFrame(instructors_q12,["instructor_id", "instructor_name"])

enrollments_q12 = [("E001", "S001", "C001"),("E002", "S002", "C002"),("E003", "S001", "C003"),("E004", "S004", "C004"),("E005", "S003", "C001"),
("E006", "S003", "C002")]
enrollments_q12_col = spark.createDataFrame(enrollments_q12,["enrollment_id", "student_id", "course_id"])


#14. Sales Performance by Region and Product
#Problem:You have four DataFrames: sales, products, regions, and salespersons. Join them to analyze the total sales for each product in different
# regions by salesperson.
salespersons_q14 = [("SP001", "Alice"),("SP002", "Bob"),("SP003", "Charlie")]
salespersons_q14_col = spark.createDataFrame(salespersons_q14,["salesperson_id", "salesperson_name"])

products_q14 = [("P001", "Laptop"),("P002", "Mobile"),("P003", "Tablet"),("P004", "Monitor")]
products_q14_col = spark.createDataFrame(products_q14,["product_id", "product_name"])

regions_q14 = [("R001", "North"),("R002", "South"),("R003", "East"),("R004", "West")]
regions_q14_col = spark.createDataFrame(regions_q14,["region_id", "region_name"])

sales_q14 = [("S001", "P001", "R001", "SP001", 800.0),("S002", "P002", "R002", "SP001", 400.0),("S003", "P003", "R003", "SP002", 300.0),
("S004", "P004", "R001", "SP002", 200.0),("S005", "P001", "R004", "SP003", 800.0),("S006", "P002", "R003", "SP003", 400.0)]
sales_q14_col = spark.createDataFrame(sales_q14,["sale_id", "product_id", "region_id", "salesperson_id", "amount"])


#15. University Students and Course Feedback
#Problem:You have four DataFrames: students, courses, instructors, and feedback. Join them to find the feedback given by students for each course
# taught by the instructors.
students_q15 = [("S001", "John"),("S002", "Emma"),("S003", "Olivia"),("S004", "Liam")]
students_q15_col = spark.createDataFrame(students_q15,["student_id", "student_name"])

courses_q15 = [("C001", "Math", "I001"),("C002", "Science", "I002"),("C003", "History", "I001"),("C004", "Art", "I003")]
courses_q15_col = spark.createDataFrame(courses_q15,["course_id", "course_name", "instructor_id"])

instructors_q15 = [("I001", "Prof. Brown"),("I002", "Prof. Green"),("I003", "Prof. White")]
instructors_q15_col = spark.createDataFrame(instructors_q15,["instructor_id", "instructor_name"])

feedback_q15 = [("F001", "S001", "C001", 4),("F002", "S002", "C002", 5),("F003", "S001", "C003", 3),("F004", "S004", "C004", 5),
                ("F005", "S003", "C001", 4),("F006", "S003", "C002", 2)]
feedback_q15_col = spark.createDataFrame(feedback_q15,["feedback_id", "student_id", "course_id", "rating"])