package org.hua;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.regex.Pattern;

public class GoodComediesPerUserRDD {

    //delimiter to split the dataset
    private static final Pattern DELIMITER = Pattern.compile("::");

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        //spark configuration and spark context
        SparkConf sparkConf = new SparkConf().setAppName("CountComediesGT3RDD");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //load ratings dataset
        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.dat");

        //RDD with Tuple2<key,Tuple2<key,value>> = Tuple2<movieId,Tuple2<userId, rating>>
        JavaPairRDD<String, Tuple2<String, Double>> ratings = ratingsLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[1];
                    String userId = lineTokens[0];
                    Double rating = Double.parseDouble(lineTokens[2]);
                    return new Tuple2<>(movieId, new Tuple2<>(userId, rating));
                });

        //load movies dataset
        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.dat");

        //RDD with Tuple2<key,Tuple2<key,value>> = Tuple2<movieId,Tuple2<title, genres>>
        JavaPairRDD<String, Tuple2<String, String>> allComedies = moviesLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[0];
                    String title = lineTokens[1];
                    String genres = lineTokens[lineTokens.length - 1];
                    return new Tuple2<>(movieId, new Tuple2<>(title, genres));
                }).filter(stringTuple2Tuple2 -> stringTuple2Tuple2._2._2.contains("Comedy"));

        //Query2: Count all movies that rated >=3.0(good) per user
        JavaPairRDD<String, Integer> goodComediesPerUser = ratings //RDD Tuple2<userId, totalRatings >=3>
                .filter(stringTuple2Tuple2 -> stringTuple2Tuple2._2._2 >= 3.0)//rating >=3.0
                .join(allComedies)
                .mapToPair(stringTuple2Tuple2 -> new Tuple2<>(stringTuple2Tuple2._2._1._1, 1)) //new Tuple2<>(userId,1)
                .reduceByKey(Integer::sum);

        //userId, totalRatingsGTE3AtComedies
        System.out.println(goodComediesPerUser.take(30));

        //write the result
        goodComediesPerUser.saveAsTextFile(args[1]+"/GoodComediesPerUserRDD");

        sc.stop();
    }
}
