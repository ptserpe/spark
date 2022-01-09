package org.hua;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.regex.Pattern;

public class GoodComediesPerUserRDD {

    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("CountComediesGT3RDD");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.csv");

        final String ratingsHeader = ratingsLines.first();

        JavaPairRDD<String, Tuple2<String, Double>> ratings = ratingsLines
                .filter(s -> !s.equals(ratingsHeader))
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[1];
                    String userId = lineTokens[0];
                    Double rating = Double.parseDouble(lineTokens[2]);
                    return new Tuple2<>(movieId, new Tuple2<>(userId, rating));
                });

        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.csv");
        final String moviesHeader = moviesLines.first();

        JavaPairRDD<String, Tuple2<String, String>> allComedies = moviesLines
                .filter(s -> !s.equals(moviesHeader))
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[0];
                    String genres = lineTokens[lineTokens.length - 1];
                    String title = l.replace(movieId + ",", "").replace(","+genres, "");
                    if (title.startsWith("\"") && title.endsWith("\""))
                        title = title.substring(1,title.length()-1);
                    return new Tuple2<>(movieId, new Tuple2<>(title, genres));
                }).filter(stringTuple2Tuple2 -> stringTuple2Tuple2._2._2.contains("Comedy"));


        JavaPairRDD<String, Integer> goodComediesPerUser = ratings
                .filter(stringTuple2Tuple2 -> stringTuple2Tuple2._2._2 >= 3.0)
                .join(allComedies)
                .mapToPair(stringTuple2Tuple2 -> new Tuple2<>(stringTuple2Tuple2._2._1._1, 1)) //new Tuple2<>(userId,1)
                .reduceByKey(Integer::sum);

        System.out.println(goodComediesPerUser.take(20));

        goodComediesPerUser.saveAsTextFile(args[1]);

        sc.stop();
    }
}
