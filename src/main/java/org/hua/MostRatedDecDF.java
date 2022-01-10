package org.hua;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MostRatedDecDF {
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
            outputPath = "output";
        } else {
            spark = SparkSession.builder().appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = args[0];
            outputPath = args[1];
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
        // ratings.csv   userId,movieId,rating,timestamp
        // movies.csv    movieId,title,genres
        // print schema
        movies.printSchema();
        ratings.printSchema();

        // print some data
//        movies.show();
//        ratings.show();

        //get month from timestamp
        Column monthColumn = month(to_timestamp(ratings.col("timestamp").cast(DataTypes.LongType)));

        Dataset<Row> mostRatedDec = ratings
                .withColumn("month", monthColumn)//add month to dataset
                .filter(col("month").equalTo(12)) //filter for December
                .groupBy(ratings.col("movieId"))
                .agg(count(ratings.col(("userId"))).alias("count_users")) //avg rating per movie
                .join(movies, "movieId") // avg rating per romantic movie
                .orderBy(col("count_users").desc()); //sort descending

        mostRatedDec.show((int)mostRatedDec.count()); //size = 22
        mostRatedDec.write().format("json").save(outputPath + "/most-rated-december-movies");

        spark.close();

    }
}
