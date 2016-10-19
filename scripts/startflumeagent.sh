#!/bin/sh
cd /home/spark/streaming
/home/spark/apache-flume-1.6.0-bin/bin/flume-ng agent --conf conf --conf-file accesslog.conf --name a1

