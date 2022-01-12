package org.hua;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class CountGoodComediesDF {
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
        Dataset<Row> movies = spark.read().option("header", "false").option("delimiter", "::").csv(inputPath+"/movies.dat");
        Dataset<Row> ratings = spark.read().option("header", "false").option("delimiter", "::").csv(inputPath+"/ratings.dat");

        //rename columns
        movies = movies
                .withColumnRenamed("_c0", "movieId")
                .withColumnRenamed("_c1", "title")
                .withColumnRenamed("_c2", "genres");

        ratings = ratings
                .withColumnRenamed("_c0", "userId")
                .withColumnRenamed("_c1", "movieId")
                .withColumnRenamed("_c2", "rating")
                .withColumnRenamed("_c3", "timestamp");
        // schema
        //
        // ratings.dat   userId,movieId,rating,timestamp
        // movies.dat    movieId,title,genres


        // get all comedies
        Dataset<Row> allComedies = movies.filter(movies.col("genres").like("%Comedy%"));

        //Count all comedies that a user rates at least 3.0
        //(join ratings with movies, filter by rating, groupby userid and
        //aggregate count)
        Dataset<Row> goodComedies = ratings
                .filter((ratings.col("rating").$greater$eq(3.0)))
                .join(allComedies, "movieId")
                .distinct();

        long totalGoodComedies = goodComedies.count();

        System.out.println("Total number of good comedies is: " + totalGoodComedies);

        //show the result
        goodComedies.show();

        //write the result
        goodComedies.write().format("json").save(outputPath+"/count-good-comedies");

        spark.close();

    }
}
