#!/bin/sh
cd /home/spark/sparkSQL
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar SparkSQLDataFrames.java
jar -cvf sparksql.jar SparkSQLDataFrames.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class SparkSQLDataFrames sparksql.jar
cd /home/spark/scripts
