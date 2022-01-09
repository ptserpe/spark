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

    private static final Pattern COMMA = Pattern.compile(",");


    public static long getMonthFromTimestamp(long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(timestamp * 1000);
        return cal.get(Calendar.MONTH) + 1;
    }

    public static void main(String[] args) {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        }
        if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("TopTenRomanticRDD").set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //load ratings.csv
        JavaRDD<String> ratingsLines = sc.textFile(args[0] + "/ratings.csv");
        final String ratingsHeader = ratingsLines.first();

        JavaPairRDD<String, Tuple3<Double, Long, Integer>> ratings = ratingsLines
                .filter(s -> !s.equals(ratingsHeader)) //remove header from data
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[1];
                    Double rating = Double.parseDouble(lineTokens[2]);
                    Long timestamp = Long.parseLong(lineTokens[3]);
                    return new Tuple2<>(movieId, new Tuple3<>(rating, timestamp, 1));
                });

        //load movies.csv
        JavaRDD<String> moviesLines = sc.textFile(args[0] + "/movies.csv");
        final String moviesHeader = moviesLines.first();
        //CASHING

        JavaPairRDD<String, Tuple2<String, String>> movieTitles = moviesLines
                .filter(s -> !s.equals(moviesHeader)) //remove header from data
                .mapToPair(l -> {
                    String[] lineTokens = COMMA.split(l);
                    String movieId = lineTokens[0];
                    String genres = lineTokens[lineTokens.length - 1];
                    String title = l.replace(movieId + ",", "").replace(","+genres, "");
                    if (title.startsWith("\"") && title.endsWith("\""))
                        title = title.substring(1,title.length()-1);
                    return new Tuple2<>(movieId, new Tuple2<>(title, genres));
                });

        //filter ratings for December,
        // reduceBy movieId and keep avg rating,
        // join with movies,
        // filter Romantics
        //sort descending to take 10 first
        JavaPairRDD<Double, Tuple2<String, String>> topTenRomantics = ratings
                .filter(stringTuple3Tuple2 -> getMonthFromTimestamp(stringTuple3Tuple2._2._2()) == 12) //find all December ratings
                .reduceByKey((doubleDateIntegerTuple3, doubleDateIntegerTuple31) -> {
                    Double newRating = doubleDateIntegerTuple3._1() + doubleDateIntegerTuple31._1(); //sum of rating values
                    Integer count = doubleDateIntegerTuple3._3() + doubleDateIntegerTuple31._3(); //number of ratings
                    return new Tuple3<>(newRating, doubleDateIntegerTuple3._2(), count); // <sum of ratings, date, num of ratings> per movieId(key)
                })
                .join(movieTitles)      //Tuple2<movieId,Tuple2<Tuple3<sum of ratings, date, num of ratings>, Tuple2<title,genres>>
                .filter(tuple2Tuple2 -> tuple2Tuple2._2._2._2.contains("Romance")) //find all romantic movies at december
                .mapToPair(stringTuple2Tuple2 -> new Tuple2<>(
                                stringTuple2Tuple2._2._1()._1() / stringTuple2Tuple2._2._1()._3(),
                                new Tuple2<>(
                                        stringTuple2Tuple2._2._2._1,
                                        stringTuple2Tuple2._2._2._2
                                )
                        )
                )
                .sortByKey(false); // Descending ordered Tuple2<avg rate,Tuple2<titles,genres>>

        //print avg rating, title, genres
        for (Tuple2<Double, Tuple2<String, String>> pair : topTenRomantics.take(10)) {
            System.out.print(pair._1);
            System.out.print(" ");
            System.out.print(pair._2._1);
            System.out.print(" ");
            System.out.println(pair._2._2);
        }

        //Save RDD as file
        topTenRomantics.saveAsTextFile(args[1]);

        sc.stop();
    }
}