import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

public class Maximum {
   public static void main(String[] args) {
      SparkConf conf = new SparkConf().setAppName("Maximum");
      JavaSparkContext jsc = new JavaSparkContext(conf);
      
      // EDIT HERE                       
      // Read randoms.txt file from HDFS 
      JavaRDD<String> dataRdd = jsc.textFile("test.txt");

      JavaRDD<Integer> newRdd = dataRdd.map(new Function<String,Integer>() {
         public Integer call(String str) {
            return Integer.parseInt(str);
         }
      }); 

      Integer sum = newRdd.reduce( new Function2<Integer,Integer,Integer>() {
         public Integer call(Integer a, Integer b) {
            // EDIT HERE   
            // Return the maximum between a and b 
            return 0;
         }
      });  
      System.out.println(sum);
   
      jsc.stop();
   }

}
