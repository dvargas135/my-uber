import mysql.connector as msc

connection = msc.connect(host="192.168.1.13", user="root", password="123456789", database="taxi_dispatch")

cursor = connection.cursor()

cursor.execute("SHOW TABLES")

records = cursor.fetchall()

print(records)

cursor.close()
connection.close()