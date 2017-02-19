import java.util.*;
import scala.Tuple2;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkContext.*;
import org.apache.spark.mllib.recommendation.*;
import org.apache.spark.rdd.RDD;

public class JavaMoviesRecommender {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaMoviesRecommender");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    if (args.length < 1) {
       System.err.println("Usage: MoviesRecommender <dir>");
       System.exit(1);
    }

    JavaRDD<String> movies = jsc.textFile(args[0]+"/movies.dat");
    Map<Integer, String> titles = movies.mapToPair( s -> {
        String[] fields = s.split("::");
        return new Tuple2<Integer, String>(Integer.parseInt(fields[0]),fields[1]);
    }).collectAsMap();

    Map<Integer, String> genres = movies.mapToPair( s -> {
        String[] fields = s.split("::");
        return new Tuple2<Integer, String>(Integer.parseInt(fields[0]),fields[2]);
    }).collectAsMap();


    JavaRDD<Rating> ratings = jsc.textFile(args[0]+"/ratings.dat").map( s -> {
        String[] fields = s.split("::");
        return new Rating(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Double.parseDouble(fields[2]));

    }).cache();
  
    Long numRatings = ratings.count();
    Long numUsers = ratings.map( r -> r.user()).distinct().count();
    Long numMovies = ratings.map( r-> r.product()).distinct().count();

    System.out.println("Got " + numRatings + " ratings, " + numUsers + " users, " + numMovies + " movies");

    MatrixFactorizationModel model = new ALS()
                .setRank(10)
                .setIterations(20)
                .setLambda(0.01)
                .setSeed(123456)
                .run(ratings);

    int userId = 1;
    int movieId = 1;  // Toy Story

    double predict = model.predict(userId,movieId);
    System.out.println("User " + userId + " will rate movie " + movieId + " " + predict);

    System.out.println("Recommended movies for user " + userId);
    Rating[] recommends = model.recommendProducts(userId,10); // top 10 movies recommended
    for (Rating r : recommends) {
       System.out.println(r.rating() + "::" + r.product() + "::" + titles.get(r.product()) + "::" + genres.get(r.product())); 
    } 

    jsc.stop();
  }

}

