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
# OLAP 1: DRILL-DOWN
# Year → Month → Day
# -----------------------------

# Year level
query_year = """
SELECT d.year, COUNT(*) AS total_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;
"""
print("\nDRILL-DOWN: YEAR LEVEL")
print(pd.read_sql(query_year, conn))

# Month level
query_month = """
SELECT d.year, d.month_name, COUNT(*) AS total_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name, d.month
ORDER BY d.year, d.month;
"""
print("\nDRILL-DOWN: MONTH LEVEL")
print(pd.read_sql(query_month, conn))

# Day level
query_day = """
SELECT d.full_date, COUNT(*) AS total_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
ORDER BY d.full_date;
"""
print("\nDRILL-DOWN: DAY LEVEL")
print(pd.read_sql(query_day, conn))

# -----------------------------
# OLAP 2: ROLL-UP
# Day → Month → Year
# -----------------------------

query_roll_month = """
SELECT d.year, d.month_name, SUM(f.quantity) AS total_quantity
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name, d.month
ORDER BY d.year, d.month;
"""
print("\nROLL-UP: MONTHLY")
print(pd.read_sql(query_roll_month, conn))

query_roll_year = """
SELECT d.year, SUM(f.quantity) AS total_quantity
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year;
"""
print("\nROLL-UP: YEARLY")
print(pd.read_sql(query_roll_year, conn))

# -----------------------------
# OLAP 3: SLICE
# -----------------------------

query_slice_digital = """
SELECT d.full_date, SUM(f.quantity) AS digital_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_resource r ON f.resource_key = r.resource_key
WHERE r.resource_category = 'Digital'
GROUP BY d.full_date;
"""
print("\nSLICE: DIGITAL RESOURCES")
print(pd.read_sql(query_slice_digital, conn))

# -----------------------------
# OLAP 4: DICE
# -----------------------------

query_dice = """
SELECT d.month_name, SUM(f.quantity) AS total_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_resource r ON f.resource_key = r.resource_key
WHERE r.resource_category = 'Digital'
  AND d.year = 2024
GROUP BY d.month_name, d.month
ORDER BY d.month;
"""
print("\nDICE: DIGITAL RESOURCES IN 2024")
print(pd.read_sql(query_dice, conn))

# -----------------------------
# Close connection
# -----------------------------
conn.close()
print("\nConnection closed")
