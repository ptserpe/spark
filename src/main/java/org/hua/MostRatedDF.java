package org.hua;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class MostRatedDF {
    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkSession spark;
        String inputPath, outputPath;
        if (isLocal) {
            spark = SparkSession.builder().master("local").appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = "src/main/resources";
            outputPath= "output";
        } else {
            spark = SparkSession.builder().appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = args[0];
            outputPath= args[1];
        }

        // load
        Dataset<Row> movies = spark.read().option("header", "true").csv(inputPath+"/movies.csv");
        Dataset<Row> links = spark.read().option("header", "true").csv(inputPath+"/links.csv");
        Dataset<Row> ratings = spark.read().option("header", "true").csv(inputPath+"/ratings.csv");
        Dataset<Row> tags = spark.read().option("header", "true").csv(inputPath+"/tags.csv");

        // schema
        //
        // ratings.csv   userId,movieId,rating,timestamp
        // movies.csv    movieId,title,genres
        // links.csv     movieId,imdbId,tmdbId
        // tags.csv      userId,movieId,tag,timestamp
        // print schema
        movies.printSchema();
        links.printSchema();
        ratings.printSchema();
        tags.printSchema();

        // print some data
//        movies.show();
//        links.show();
//        ratings.show();
//        tags.show();

        // TODO: count all comedies that a user rates at least 3.0
        //       (join ratings with movies, filter by rating, groupby userid and
        //        aggregate count)
        Dataset<Row> mostRatedMovies = ratings
                .join(movies, "movieId")
                .groupBy(movies.col("title"))
                .agg(count("*").alias("count"))
                .orderBy(col("count").desc())
                .limit(25);


        mostRatedMovies.show(25);
        //mostRatedMovies.write().format("json").save(outputPath+"/good-comedies-per-user");

        spark.close();

    }
}
