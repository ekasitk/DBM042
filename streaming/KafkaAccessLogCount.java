import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

/**
 * Usage: KafkaAccessLogCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 */

public final class KafkaAccessLogCount {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaAccessLogCount <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    String brokers = args[0];
    String topics = args[1];

    Duration batchInterval = Durations.seconds(15);
    SparkConf conf = new SparkConf().setAppName("KafkaAccessLogCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, batchInterval);

    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Create direct kafka stream with brokers and topics
    // message is a pair of (Key,Value)
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    messages.count().map(new Function<Long, String>() {
      @Override
      public String call(Long in) {
        return "Received " + in + " kafka messages.";
      }
    }).print();

    JavaDStream<ApacheAccessLog> accessLogDS = messages.map(new Function<Tuple2<String,String>, ApacheAccessLog>() {
       @Override
       public  ApacheAccessLog call(Tuple2<String,String> tuple) {
          String line = tuple._2();
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

    jssc.start();
    jssc.awaitTermination();

  }
}
