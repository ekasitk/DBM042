import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;

public final class JavaAccessLogCount {

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaAccessLogCount <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaAccessLogCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0],4);

    JavaRDD<ApacheAccessLog> accesses = lines.map(new Function<String, ApacheAccessLog>() {
      @Override
      public ApacheAccessLog call(String s) {
        String[] cols = s.split(",");
        return new ApacheAccessLog(cols[0],cols[1],cols[2],cols[3],cols[4],cols[5],cols[6],Integer.parseInt(cols[7]),Long.parseLong(cols[8]),cols[9],cols[10]);
      }
    });

    JavaPairRDD<String, Integer> ones = accesses.mapToPair(new PairFunction<ApacheAccessLog, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(ApacheAccessLog access) {
        return new Tuple2<String, Integer>(access.ipAddress, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    ctx.stop();
  }
}
