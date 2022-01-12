package org.hua;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class TopTenRomanticRDD {

    private static final Pattern DELIMITER = Pattern.compile("::");

    //get month from timestamp
    public static long getMonthFromTimestamp(long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(timestamp * 1000);
        return cal.get(Calendar.MONTH) + 1; //we add 1 to result, as Calendar.MONTH range is 0-11
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("TopTenRomanticRDD");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //load ratings.csv
        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.dat");

        JavaPairRDD<String, Tuple3<Double, Long, Integer>> ratings = ratingsLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[1];
                    Double rating = Double.parseDouble(lineTokens[2]);
                    Long timestamp = Long.parseLong(lineTokens[3]);
                    return new Tuple2<>(movieId, new Tuple3<>(rating, timestamp, 1));
                });

        //load movies.csv
        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.dat");
        //CASHING

        JavaPairRDD<String, Tuple2<String, String>> movieTitles = moviesLines
                .mapToPair(l -> {
                    String[] lineTokens = DELIMITER.split(l);
                    String movieId = lineTokens[0];
                    String title = lineTokens[1];
                    String genres = lineTokens[lineTokens.length - 1];
                    return new Tuple2<>(movieId, new Tuple2<>(title, genres));
                });

        //Query3: Find the top 10 romantic movies for December
        //filter ratings for December,
        // reduceBy movieId and keep avg rating,
        // join with movies,
        // filter Romantics
        //sort descending to take 10 first
        JavaPairRDD<Double, Tuple2<String, String>> topRomantics = ratings
                .filter(ratingTimestampCountPerMovieId -> getMonthFromTimestamp(ratingTimestampCountPerMovieId._2._2()) == 12) //find all December ratings
                .reduceByKey((ratingDecemberCountPerMovieId, ratingDecemberCountPerMovieIdNext) -> {
                    Double newRating = ratingDecemberCountPerMovieId._1() + ratingDecemberCountPerMovieIdNext._1(); //sum of rating values
                    Integer count = ratingDecemberCountPerMovieId._3() + ratingDecemberCountPerMovieIdNext._3(); //number of ratings
                    return new Tuple3<>(newRating, ratingDecemberCountPerMovieId._2(), count); // <sum of ratings, date, num of ratings> per movieId(key)
                })
                .join(movieTitles)      //Tuple2<movieId,Tuple2<Tuple3<sum of ratings, date, num of ratings>, Tuple2<title,genres>>
                .filter(joinRatingsWithMovieTitlesOutput -> joinRatingsWithMovieTitlesOutput._2._2._2.contains("Romance")) //find all romantic movies at december
                .mapToPair(joinRatingsWithRomanticOutput -> new Tuple2<>(
                                joinRatingsWithRomanticOutput._2._1()._1() / joinRatingsWithRomanticOutput._2._1()._3(), //find the avg rating
                                new Tuple2<>(
                                        joinRatingsWithRomanticOutput._2._2._1, //title
                                        joinRatingsWithRomanticOutput._2._2._2 //genres
                                )
                        )
                )
                .sortByKey(false); // Descending ordered Tuple2<avg rate,Tuple2<titles,genres>>

        JavaRDD<Tuple2<Double, Tuple2<String, String>>> topTenRomantics = sc.parallelize(topRomantics.take(10));

        //print avg rating, title, genres
        for (Tuple2<Double, Tuple2<String, String>> pair : topTenRomantics.take(10)) {
            System.out.println("Average rating: " + pair._1 + ", movie title: " + pair._2._1 + ", genres: " + pair._2._2);
        }

        //Save RDD as file
        topTenRomantics.saveAsTextFile(args[1] + "/TopTenRomanticRDD");

        sc.stop();
    }
}