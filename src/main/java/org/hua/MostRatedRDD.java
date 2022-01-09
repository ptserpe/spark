package org.hua;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class MostRatedRDD {

    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("MostRatedRDD").set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.csv");

        final String ratingsHeader = ratingsLines.first();

        JavaPairRDD<String, Integer> movieIds = ratingsLines
                .filter(s -> !s.equals(ratingsHeader))
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[1];
                    return new Tuple2<>(movieId, 1);
                });

        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.csv");
        final String moviesHeader = moviesLines.first();

        JavaPairRDD<String, String> movieTitles = moviesLines
                .filter(s -> !s.equals(moviesHeader))
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[0];
                    String genres = lineTokens[lineTokens.length - 1];
                    String title = l.replace(movieId + ",", "").replace(","+genres, "");
                    if (title.startsWith("\"") && title.endsWith("\""))
                        title = title.substring(1,title.length()-1);
                    return new Tuple2<>(movieId, title);
                });

        JavaPairRDD<Integer, String> mostRatedMovies = movieIds.reduceByKey(Integer::sum)
                .join(movieTitles)
                .mapToPair(stringTuple2Tuple2 -> new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2))
                .sortByKey(false);

        for(Tuple2<Integer, String> pair:mostRatedMovies.take(25)) {
            System.out.print(pair._1);
            System.out.println(pair._2);
        }

        mostRatedMovies.saveAsTextFile(args[1]);

        sc.stop();
    }
}
