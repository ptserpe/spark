package org.hua;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class TopTenRomanticDF {
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

        // get all comedies
        Dataset<Row> allRomantics = movies.filter(movies.col("genres").like("%Romance%"));

        Column monthColumn = month(to_timestamp(ratings.col("timestamp").cast(DataTypes.LongType)));

        Dataset<Row> topTenRomantics = ratings
                .withColumn("month", monthColumn)
                .filter(col("month").equalTo(12)) //filter for December
                .groupBy(ratings.col("movieId"))
                .agg(avg(ratings.col(("rating"))).alias("avg_rating")) //avg rating per movie
                .join(allRomantics, "movieId") // avg rating per romantic movie
                .orderBy(col("avg_rating").desc()); //sort descending to take top 10

//        long size = topTenRomantics.count();
//        System.out.println(size);

        //topTenRomantics.show((int)size);
        topTenRomantics.show(22);
        //topTenRomantics.write().format("json").save(outputPath+"/top-ten-romantics");

        spark.close();

    }
}
