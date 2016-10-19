#!/bin/sh
cd /home/spark/pi
javac -cp $SPARK_HOME/lib/spark-assembly-1.6.2-hadoop2.6.0.jar GregoryPi.java
jar -cvf gregorypi.jar GregoryPi*.class
$SPARK_HOME/bin/spark-submit --master spark://spark-master:7077 --class GregoryPi gregorypi.jar > /tmp/pi.txt
cd /home/spark/scripts
