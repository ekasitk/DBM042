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
      
      JavaRDD<String> dataRdd = jsc.textFile("/dataset/randoms.txt");

      JavaRDD<Integer> newRdd = dataRdd.map(new Function<String,Integer>() {
         public Integer call(String str) {
            return Integer.parseInt(str);
         }
      }); 

      Integer sum = newRdd.reduce( new Function2<Integer,Integer,Integer>() {
         public Integer call(Integer a, Integer b) {
            // Return the maximum between a and b
            return a > b ? a : b; 
         }
      });  
      System.out.println(sum);
   
      jsc.stop();
   }

}
