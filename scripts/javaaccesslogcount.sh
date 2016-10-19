#!/bin/sh
cd /home/spark/accesslog
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar JavaAccessLogCount.java ApacheAccessLog.java
jar -cvf accesslog.jar ApacheAccessLog.class JavaAccessLogCount*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class JavaAccessLogCount accesslog.jar /dataset/accesslog.csv > /tmp/access.txt
cd /home/spark/scripts
