import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GregoryPi {
   public static void main(String[] args) {
      SparkConf conf = new SparkConf().setAppName("Gregory Pi");
      JavaSparkContext jsc = new JavaSparkContext(conf);
      
      Integer[] arr = new Integer[100000];
      for (int i=0; i < 100000; i++) arr[i] = i; 

      JavaRDD<Integer> dataRdd = jsc.parallelize(Arrays.asList(arr),4);

      JavaRDD<Double> newRdd = dataRdd.map(new Function<Integer,Double>() {
         public Double call(Integer i) {
            int divisor = 2*i+1;
            if (i % 2 != 0) divisor = -divisor;
            return 1.0/divisor;
         }
      }); 

      Double sum = newRdd.reduce(new Function2<Double,Double,Double>() {
         public Double call(Double i, Double j) {
            return i+j;
         }
      });

      System.out.println(sum*4);

      jsc.stop();
   }

}
