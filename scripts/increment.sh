#!/bin/sh
cd /home/spark/ones
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar Increment.java
jar -cvf inc.jar Increment*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class Increment inc.jar
cd /home/spark/scripts
