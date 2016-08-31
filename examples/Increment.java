import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

public class Increment {
   public static void main(String[] args) {
      SparkConf conf = new SparkConf().setAppName("Increment");
      JavaSparkContext jsc = new JavaSparkContext(conf);
      
      JavaRDD<String> dataRdd = jsc.textFile("/dataset/ones.txt");

      JavaRDD<Integer> newRdd = dataRdd.map(new Function<String,Integer>() {
         public Integer call(String str) {
            return 1 + Integer.parseInt(str);
         }
      }); 

/*
      Integer sum = newRdd.reduce( new Function2<Integer,Integer,Integer>() {
         public Integer call(Integer i, Integer j) {
            return i+j;
         }
      });  
      System.out.println(sum);
*/
   
      newRdd.saveAsTextFile("/user/spark/output");

      jsc.stop();
   }

}
