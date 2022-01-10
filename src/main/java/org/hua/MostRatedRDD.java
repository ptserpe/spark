package org.hua;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MostRatedRDD {

    private static final Pattern DELIMITER = Pattern.compile("::");

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        //spark configuration and spark context
        SparkConf sparkConf = new SparkConf().setAppName("MostRatedRDD"); //.setMaster("local[2]").set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //load ratings dataset
        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.dat");

        //RDD with <key,value> = <movieId,1>
        JavaPairRDD<String, Integer> movieIds = ratingsLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[1];
                    return new Tuple2<>(movieId, 1);
                });

        //load movies dataset
        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.dat");

        //RDD with <key,value> = <movieId,title>
        JavaPairRDD<String, String> movieTitles = moviesLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[0];
                    String title = lineTokens[1];
                    return new Tuple2<>(movieId, title);
                });

        //Query1: Find the 25 most rated movies using RDD
        JavaPairRDD<Integer, String> mostRatedMovies = movieIds.reduceByKey(Integer::sum) //find total ratings for each movieId
                .join(movieTitles)//join to take the title. RDD Tuple2<movieId, Tuple2<totalRatings, title>>
                .mapToPair(stringTuple2Tuple2 -> new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2))//Keep RDD Tuple2<totalRatings, title>
                .sortByKey(false); //sort by key descending to take the first 25

        //show the result
        for(Tuple2<Integer, String> pair:mostRatedMovies.take(25)) {
            System.out.println("Total ratings: " + pair._1 + ", Movie title: " + pair._2);
        }

        //write the result
        mostRatedMovies.saveAsTextFile(args[1] + "/MostRatedRDD");

        sc.stop();
    }
}
