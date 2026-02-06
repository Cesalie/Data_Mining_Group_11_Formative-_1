import mysql.connector
import pandas as pd

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
# 1. Total book transactions
# -----------------------------
query1 = "SELECT COUNT(*) AS total_books FROM fact_library_usage WHERE purpose='Book Transaction';"
df1 = pd.read_sql(query1, conn)
print("\n1️⃣ Total Book Transactions")
print(df1)

# -----------------------------
# 2. Total digital downloads by resource type
# -----------------------------
query2 = """
SELECT r.resource_type, SUM(f.quantity) AS total_downloads
FROM fact_library_usage f
JOIN dim_resource r ON f.resource_key = r.resource_key
WHERE f.purpose='Digital Usage'
GROUP BY r.resource_type
ORDER BY total_downloads DESC;
"""
df2 = pd.read_sql(query2, conn)
print("\n2️⃣ Total Digital Downloads by Resource Type")
print(df2)

# -----------------------------
# 3. Average duration per room booking
# -----------------------------
query3 = """
SELECT r.room_number, AVG(f.duration_minutes) AS avg_duration
FROM fact_library_usage f
JOIN dim_room r ON f.room_key = r.room_key
WHERE f.room_key IS NOT NULL
GROUP BY r.room_number
ORDER BY avg_duration DESC;
"""
df3 = pd.read_sql(query3, conn)
print("\n3️⃣ Average Duration per Room Booking")
print(df3)

# -----------------------------
# 4. Max and Min downloads per month
# -----------------------------
query4 = """
SELECT d.month_name, MAX(f.quantity) AS max_usage, MIN(f.quantity) AS min_usage
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.purpose='Digital Usage'
GROUP BY d.month_name
ORDER BY FIELD(d.month_name,
  'January','February','March','April','May','June','July','August','September','October','November','December');
"""
df4 = pd.read_sql(query4, conn)
print("\n4️⃣ Max & Min Downloads per Month")
print(df4)

# -----------------------------
# 5. Total usage by department
# -----------------------------
query5 = """
SELECT dep.department_name, SUM(f.quantity) AS total_usage
FROM fact_library_usage f
JOIN dim_department dep ON f.department_key = dep.department_id
GROUP BY dep.department_name
ORDER BY total_usage DESC;
"""
df5 = pd.read_sql(query5, conn)
print("\n5️⃣ Total Usage by Department")
print(df5)

# -----------------------------
# 6. Monthly usage trend for E-Books
# -----------------------------
query6 = """
SELECT d.year, d.month_name, SUM(f.quantity) AS monthly_downloads
FROM fact_library_usage f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_resource r ON f.resource_key = r.resource_key
WHERE r.resource_type='E-Book'
GROUP BY d.year, d.month_name
ORDER BY d.year, MONTH(STR_TO_DATE(d.month_name, '%M'));
"""
df6 = pd.read_sql(query6, conn)
print("\n6️⃣ Monthly Usage Trend for E-Books")
print(df6)

# -----------------------------
# 7. Top 5 students by total usage
# -----------------------------
query7 = """
SELECT s.student_id, s.student_type, SUM(f.quantity) AS total_usage
FROM fact_library_usage f
JOIN dim_student s ON f.student_key = s.student_key
GROUP BY s.student_id
ORDER BY total_usage DESC
LIMIT 5;
"""
df7 = pd.read_sql(query7, conn)
print("\n7️⃣ Top 5 Students by Total Usage")
print(df7)

# -----------------------------
# 8. Rank departments by digital usage
# -----------------------------
query8 = """
SELECT dep.department_name, SUM(f.quantity) AS digital_usage,
       RANK() OVER (ORDER BY SUM(f.quantity) DESC) AS rank
FROM fact_library_usage f
JOIN dim_department dep ON f.department_key = dep.department_id
JOIN dim_resource r ON f.resource_key = r.resource_key
WHERE f.purpose='Digital Usage'
GROUP BY dep.department_name;
"""
df8 = pd.read_sql(query8, conn)
print("\n8️⃣ Departments Ranked by Digital Usage")
print(df8)

# -----------------------------
# 9. Comparison of room vs digital usage
# -----------------------------
query9 = """
SELECT
    SUM(CASE WHEN f.room_key IS NOT NULL THEN f.quantity ELSE 0 END) AS total_room_usage,
    SUM(CASE WHEN f.purpose='Digital Usage' THEN f.quantity ELSE 0 END) AS total_digital_usage
FROM fact_library_usage f;
"""
df9 = pd.read_sql(query9, conn)
print("\n9️⃣ Comparison of Room vs Digital Usage")
print(df9)

# -----------------------------
# 10. Total usage per resource category per department
# -----------------------------
query10 = """
SELECT dep.department_name, r.resource_category, SUM(f.quantity) AS total_usage
FROM fact_library_usage f
JOIN dim_department dep ON f.department_key = dep.department_id
JOIN dim_resource r ON f.resource_key = r.resource_key
GROUP BY dep.department_name, r.resource_category
ORDER BY dep.department_name, total_usage DESC;
"""
df10 = pd.read_sql(query10, conn)
print("\n🔟 Total Usage per Resource Category per Department")
print(df10)

# -----------------------------
# 11. Average usage per student type per resource type
# -----------------------------
query11 = """
SELECT s.student_type, r.resource_type, AVG(f.quantity) AS avg_usage
FROM fact_library_usage f
JOIN dim_student s ON f.student_key = s.student_key
JOIN dim_resource r ON f.resource_key = r.resource_key
GROUP BY s.student_type, r.resource_type
ORDER BY s.student_type;
"""
df11 = pd.read_sql(query11, conn)
print("\n1️⃣1️⃣ Average Usage per Student Type per Resource Type")
print(df11)

# -----------------------------
# Close Connection
# -----------------------------
conn.close()
print("\nConnection closed")
