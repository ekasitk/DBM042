import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public final class UncacheLogCount {

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: UncacheLogCount <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("UncacheLogCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0],4);

    JavaRDD<ApacheAccessLog> accesses = lines.map(new Function<String, ApacheAccessLog>() {
      @Override
      public ApacheAccessLog call(String s) {
        String[] cols = s.split(",");
        return new ApacheAccessLog(cols[0],cols[1],cols[2],cols[3],cols[4],cols[5],cols[6],Integer.parseInt(cols[7]),Long.parseLong(cols[8]),cols[9],cols[10]);
      }
    });
    //accesses.cache();

    Long numTotal = accesses.count();
    System.out.println("Total = " + numTotal);

    JavaRDD<ApacheAccessLog> errorLog = accesses.filter(new Function<ApacheAccessLog, Boolean>() {
      @Override
      public Boolean call(ApacheAccessLog log) {
        if (log.responseCode != 200) 
           return true;
        else 
           return false; 
      }
    });
    System.out.println("Error = " + errorLog.count());
   
    ctx.stop();
  }
}
