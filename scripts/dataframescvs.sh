#!/bin/sh
cd /home/spark/sparkSQL
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar:spark-csv_2.10-1.4.0.jar SparkSQLDataFrames.java
jar -cvf sparksql.jar SparkSQLDataFrames.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class SparkSQLDataFrames --jars spark-csv_2.10-1.4.0.jar,commons-csv-1.4.jar sparksql.jar
cd /home/spark/scripts
