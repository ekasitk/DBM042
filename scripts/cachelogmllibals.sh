#!/bin/sh
cd /home/spark/als
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar ApacheAccessLog.java CacheLogMlLibALS.java
jar -cvf als.jar ApacheAccessLog.class CacheLogMlLibALS*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class CacheLogMlLibALS als.jar /dataset/accesslog.csv
cd /home/spark/scripts
