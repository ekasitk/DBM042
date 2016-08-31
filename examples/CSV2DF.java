import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

public final class CSV2DF {

  public static void main(String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("CSV2DF");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new HiveContext(ctx);  

    StructType schema = new StructType().add("ipaddress","String")
                                        .add("clientid","String")
                                        .add("userid","String")
                                        .add("datetime","Timestamp")
                                        .add("method","String")
                                        .add("endpoint","String")
                                        .add("protocol","String")
                                        .add("responsecode","Integer")
                                        .add("contentsize","Integer")
                                        .add("referer","String")
                                        .add("useragent","String");

    DataFrame csvDF = sqlContext.read()
                                .format("com.databricks.spark.csv")
                                .option("header","false")
                                .schema(schema)
                                .load("/dataset/accesslog.csv");
              
    csvDF.printSchema();
    csvDF.show();

    ctx.stop();
  }
}
