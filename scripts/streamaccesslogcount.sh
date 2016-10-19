#!/bin/sh
cd /home/spark/streaming
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar:$SPARK_HOME/lib/spark-examples-1.6.2-hadoop2.6.0.jar StreamAccessLogCount.java AccessLogParser.java ApacheAccessLog.java
jar -cvf streaming.jar *.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class StreamAccessLogCount --jars $SPARK_HOME/lib/spark-examples-1.6.2-hadoop2.6.0.jar streaming.jar spark-master 5000
cd /home/spark/scripts
