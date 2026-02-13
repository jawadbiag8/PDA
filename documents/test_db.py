import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="kpi_monitoring"
)

print("DB connected successfully")
conn.close()