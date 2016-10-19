import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

public final class CacheLogMlLibALS {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
      			System.err.println("Usage: CacheLogMlLibALS <file>");
      			System.exit(1);
    		}

    	SparkConf sparkConf = new SparkConf().setAppName("CacheLogMlLibALS");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	JavaRDD<String> lines = ctx.textFile(args[0],4);
	//load data
	JavaRDD<ApacheAccessLog> accesses = lines.map(new Function<String, ApacheAccessLog>() {
      	@Override
      		public ApacheAccessLog call(String s) {
        		String[] cols = s.split(",");
        		return new ApacheAccessLog(cols[0],cols[1],cols[2],cols[3],cols[4],cols[5],cols[6],Integer.parseInt(cols[7]),Long.parseLong(cols[8]),cols[9],cols[10]);
      		}	
    	});
    	accesses.cache();
    	//amount of data
	System.out.println("AccessLog.count:	"+accesses.count());

	//create RDD of each feature: ipAddress
	JavaPairRDD<String, Long> ipAddressRDD_dist_zip = accesses.map(new Function<ApacheAccessLog, String>(){
		List<String> temp = new ArrayList<String>();
		String result = "";
		public String call(ApacheAccessLog log){
			if(!temp.contains(log.ipAddress)){
				temp.add(log.ipAddress);
				result = log.ipAddress;		
			}
			return result;	
		}
	}).distinct().zipWithUniqueId();	
	ipAddressRDD_dist_zip.cache();	
	System.out.println("ipAddress:	" + ipAddressRDD_dist_zip.count());
	final Map<String, Long> ipAddressMap = ipAddressRDD_dist_zip.collectAsMap();

	//endpoint
	JavaPairRDD<String, Long> endpointRDD_dist_zip = accesses.map(new Function<ApacheAccessLog, String>(){
                List<String> temp = new ArrayList<String>();
                String result = "";
                public String call(ApacheAccessLog log){
                        if(!temp.contains(log.endpoint)){
                                temp.add(log.endpoint);
                                result = log.endpoint;
                        }
                        return result;
                }
        }).distinct().zipWithUniqueId();
	endpointRDD_dist_zip.cache();
	final Map<String, Long> endpointMap = endpointRDD_dist_zip.collectAsMap();	
        System.out.println("EndpointMap:	"+endpointMap.size());
	
	//Rating: data preparation
	JavaPairRDD<String, Integer> dataPrep = accesses.mapToPair(new PairFunction<ApacheAccessLog, String, Integer>(){
		public Tuple2<String, Integer> call(ApacheAccessLog log){
			int user = ipAddressMap.get(log.ipAddress).intValue();
                        int product = endpointMap.get(log.endpoint).intValue();
			String keyRating = user+""+":"+product+"";
			return new Tuple2<String, Integer>(keyRating, 1);
		}
	});
	//Rating: data preparation -> rating.ReduceByKey
	JavaPairRDD<String, Integer> reducedRating = dataPrep.reduceByKey(new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer i1, Integer i2) {
			return i1 + i2;
		}
	});
	System.out.println("reduceRating:	"+reducedRating.count());
	System.out.println("reduceRating.first() " + reducedRating.first());	

	//Rating is created
	JavaRDD<Rating> implicitRating = reducedRating.map(new Function<Tuple2<String, Integer>, Rating>(){
		public Rating call(Tuple2<String, Integer> d){
			String[] sarray = d._1().split(":");
			return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),d._2().doubleValue());
		}
	}).cache();
	System.out.println("implicitRating:	"+implicitRating.count());
	System.out.println("implicitRating.first():	"+implicitRating.first());
	
	//Build the recommendation model using ALS
	MatrixFactorizationModel model = new ALS().setRank(10).setIterations(20).setLambda(0.01).setAlpha(1.0).setSeed(100).run(JavaRDD.toRDD(implicitRating));
	
	// swap ipAddressMap and endpointMap
	JavaPairRDD<Long, String> swappedIpAddressMap = ipAddressRDD_dist_zip.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
		public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
			return item.swap();
		}	
	});
	final Map<Long, String> ipAddressXmap = swappedIpAddressMap.collectAsMap();

        JavaPairRDD<Long, String> swappedEndpointMap = endpointRDD_dist_zip.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
                public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
                        return item.swap();
                }
        });
        final Map<Long, String> endpointXmap = swappedEndpointMap.collectAsMap();

	System.out.println("user's IP address : "+ipAddressXmap.get(13425));
	System.out.println("endpoint : "+endpointXmap.get(87));

	// Test: predict one endpoint        
	double predict = model.predict(13425,87);        
	System.out.println("user: " + predict); 

	// Test: recommend 5 endpoints for 13425
	Rating[] des = model.recommendProducts(13425,5);
	for (Rating r : des){
		System.out.println(r);
		System.out.println(ipAddressXmap.get(r.user()) +" ;; " + endpointXmap.get(r.product()) + " ;; " + r.rating());
		System.out.println("====================================================================");
	}
	
	}
}

