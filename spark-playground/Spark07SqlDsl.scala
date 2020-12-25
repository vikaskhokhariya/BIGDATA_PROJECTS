package ca.mcit.bigdata.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col}

object Spark07SqlDsl extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder().appName("SparkSqlDsl").master("local[*]").getOrCreate()

  val movieDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/movie/movie.csv")
  val ratingDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/rating/rating.csv")

  val movieRatingAfter2000 =
    movieDf
      .join(ratingDf,"mID")
      .where(col("year") > 2000)
      .groupBy(col("title"))
      .agg(avg("stars").as("Average Rating"))

  movieRatingAfter2000.show()
  movieRatingAfter2000.coalesce(1).write.mode(SaveMode.Overwrite).csv("/user/bdss2001/mrvikkku/getMovieDataFromSpark")

  spark.stop()
}
