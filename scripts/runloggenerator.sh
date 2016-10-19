#!/bin/sh
cd /home/spark/streaming
javac SampleLogGenerator.java
java -cp . SampleLogGenerator accesslog.raw /tmp/access.log
