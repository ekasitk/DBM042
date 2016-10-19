#!/bin/sh
cd /home/spark/cache
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar CacheLogCount.java UncacheLogCount.java ApacheAccessLog.java
jar -cvf cache.jar ApacheAccessLog.class CacheLogCount*.class UncacheLogCount*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --executor-memory 2g --class UncacheLogCount cache.jar /dataset/biglog.csv
cd /home/spark/scripts
