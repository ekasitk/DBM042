import java.util.List;
import java.util.Properties;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;

public final class SparkSQLDataFrames {
        private static final String MYSQL_USERNAME = "spark";
        private static final String MYSQL_PWD = "spark";
        private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://spark-master:3306/apachelog";

        public static void main(String[] args) throws Exception {
                SparkConf sparkConf = new SparkConf().setAppName("SparkSQLDataFrames");
                JavaSparkContext sc = new JavaSparkContext(sparkConf);

                // Initialize the SQLContext
                //SQLContext sqlContext = new SQLContext(sc);
                HiveContext sqlContext = new HiveContext(sc);
                Properties properties = new Properties();
                properties.put("user", MYSQL_USERNAME);
                properties.put("password", MYSQL_PWD);

                //String SQLString = "(select ipaddress, responsecode from apachelog.accesslog) as subset";
                String SQLString = "accesslog";
                // Load MySQL query result as DataFrame
                DataFrame jdbcDF = sqlContext.read().jdbc(MYSQL_CONNECTION_URL, SQLString, properties);

                jdbcDF.printSchema();
                jdbcDF.show();
                jdbcDF.describe("ipaddress","contentsize").show();

/*              // Using SQL command
                jdbcDF.registerTempTable("accesslog");
                DataFrame errorLog = sqlContext.sql("select ipaddress, responsecode,contentsize from accesslog where responsecode <> 200");
                errorLog.show();

                // Save as CSV format
                errorLog.write()
                        .format("com.databricks.spark.csv")
                        .option("header","false")
                        .save("/user/spark/errorLog");
*/

/*
                // Working with each row
                Row[] rows = errorLog.collect();
                for (Row row: rows) {
                   String ip = row.getAs("ipaddress");
                   int size = row.getAs("contentsize");
                   System.out.println(ip + "," + size);
                }
*/

                sc.stop();
        }
}

