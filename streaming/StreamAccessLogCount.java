import scala.Tuple2;
import java.util.*;
import java.io.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class StreamAccessLogCount {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: StreamAccessLogCount <host> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    Duration batchInterval = Durations.seconds(15);
    SparkConf conf = new SparkConf().setAppName("StreamAccessLogCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(jssc, host, port);

    flumeStream.count().map(new Function<Long, String>() {
      @Override
      public String call(Long in) {
        return "Received " + in + " flume events.";
      }
    }).print();

    JavaDStream<ApacheAccessLog> accessLogDS = flumeStream.map(new Function<SparkFlumeEvent, ApacheAccessLog>() {
       @Override
       public  ApacheAccessLog call(SparkFlumeEvent fe) {
          String line = new String(fe.event().getBody().array());
          return AccessLogParser.parseFromLogLine(line);
       }
    });

    JavaPairDStream<Integer,Integer> responsesDS = accessLogDS.mapToPair(
       new PairFunction<ApacheAccessLog,Integer,Integer>() {
          public Tuple2<Integer,Integer> call(ApacheAccessLog access) {
             return new Tuple2<Integer,Integer>(access.responseCode,1);
          }
       }
    );
    
    JavaPairDStream<Integer,Integer> countsDS = responsesDS.reduceByKey(
       new Function2<Integer,Integer,Integer>() {
          public Integer call(Integer a, Integer b) {
             return a + b;
          }
       }
    );

    countsDS.print();

    // Window Operation
/*
    Function2 reduceFunc = new Function2<Integer,Integer,Integer>() {
                                 public Integer call(Integer a, Integer b) {
                                    return a + b;
                                 }
                               }; 
    JavaPairDStream<Integer,Integer> windowedCountsDS = responsesDS.reduceByKeyAndWindow(reduceFunc, Durations.seconds(60) );

    windowedCountsDS.print();
*/

    // Save to local file
/*
    countsDS.foreachRDD( 
        new Function2<JavaPairRDD<Integer,Integer>,Time,Void>() {
           @Override
           public Void call(JavaPairRDD<Integer,Integer> rdd, Time t) throws Exception {
              List<Tuple2<Integer,Integer>> tuples = rdd.collect();
              String filename = "/tmp/responseCount-" + t.milliseconds();
              PrintWriter pw = new PrintWriter(filename);
              for (Tuple2 tuple : tuples) {
                  pw.println(tuple._1 + "," + tuple._2);
              }
              pw.close();
              return null;
           }
        }
    );
*/

    jssc.start();
    jssc.awaitTermination();
  }
}
