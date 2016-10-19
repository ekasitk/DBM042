#!/bin/sh
spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.JavaWordCount $SPARK_HOME/lib/spark-examples-1.6.2-hadoop2.6.0.jar hdfs://spark-master/tmp/hosts
