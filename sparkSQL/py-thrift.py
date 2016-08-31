#!//opt/python27/bin/python
from pyhive import hive
conn = hive.Connection(host="spark-master", port=10000, username="spark", database='apachelog')
cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM accesslog where responsecode <> 200")
for result in cursor.fetchall():
	print result
