import pandas as pd
import mysql.connector

# -----------------------------
# Database Connection
# -----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="university library analytics",
    auth_plugin="mysql_native_password"
)

print("Connected to database")

# -----------------------------
# PIVOT VIEW 1
# Resource Type × Year
# -----------------------------
query1 = """
SELECT d.year, r.resource_type, f.quantity
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_resource r ON f.resource_key = r.resource_key;
"""
df1 = pd.read_sql(query1, conn)

pivot_resource_year = pd.pivot_table(
    df1,
    values="quantity",
    index="resource_type",
    columns="year",
    aggfunc="sum",
    fill_value=0
)

print("\nPIVOT 1: Resource Type × Year")
print(pivot_resource_year)

# -----------------------------
# PIVOT VIEW 2
# Department × Resource Type
# -----------------------------
query2 = """
SELECT dep.department_name, r.resource_type, f.quantity
FROM fact_library_usage f
JOIN dim_department dep ON f.department_key = dep.department_id
JOIN dim_resource r ON f.resource_key = r.resource_key;
"""
df2 = pd.read_sql(query2, conn)

pivot_dept_resource = pd.pivot_table(
    df2,
    values="quantity",
    index="department_name",
    columns="resource_type",
    aggfunc="sum",
    fill_value=0
)

print("\nPIVOT 2: Department × Resource Type")
print(pivot_dept_resource)

# -----------------------------
# PIVOT VIEW 3
# Student Type × Resource Category
# -----------------------------
query3 = """
SELECT s.student_type, r.resource_category, f.quantity
FROM fact_library_usage f
JOIN dim_student s ON f.student_key = s.student_key
JOIN dim_resource r ON f.resource_key = r.resource_key;
"""
df3 = pd.read_sql(query3, conn)

pivot_student_resource = pd.pivot_table(
    df3,
    values="quantity",
    index="student_type",
    columns="resource_category",
    aggfunc="sum",
    fill_value=0
)

print("\nPIVOT 3: Student Type × Resource Category")
print(pivot_student_resource)

# -----------------------------
# PIVOT VIEW 4
# Month × Department
# -----------------------------
query4 = """
SELECT d.month_name, dep.department_name, f.quantity
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_department dep ON f.department_key = dep.department_id;
"""
df4 = pd.read_sql(query4, conn)

pivot_month_department = pd.pivot_table(
    df4,
    values="quantity",
    index="month_name",
    columns="department_name",
    aggfunc="sum",
    fill_value=0
)

print("\nPIVOT 4: Month × Department")
print(pivot_month_department)

# -----------------------------
# Close connection
# -----------------------------
conn.close()
print("\nConnection closed")
