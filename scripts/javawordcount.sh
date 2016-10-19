#!/bin/sh
cd /home/spark/wordcount
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar JavaWordCount.java
jar -cvf testjwc.jar JavaWordCount*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class JavaWordCount testjwc.jar hdfs://spark-master/dataset/William.txt > /tmp/wordcount.txt
cd /home/spark/scripts
