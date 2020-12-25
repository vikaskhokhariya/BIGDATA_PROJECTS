package ca.mcit.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03CoreHdfs extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Core Practice HDFS")
  val sparkContext = new SparkContext(sparkConf)

  val inputRddForMovie = sparkContext.textFile("/user/bdss2001/mrvikkku/movie.csv")
  val movieRdd: RDD[Movie] = inputRddForMovie.filter(!_.startsWith("mID"))map(Movie(_))

  val inputRddForRating = sparkContext.textFile("/user/bdss2001/mrvikkku/rating/rating.csv")
  val ratingRdd = inputRddForRating.filter(!_.startsWith("rID")).map(Rating(_))

  //(101, Movie(101,Gone with the wind,1939, Victor Fleming))
  val x = movieRdd.keyBy(movie => movie.mID)

  //(101, Rating(201, 101, 2, 2011-01-22))
  val y = ratingRdd.keyBy(rating => rating.mID)

  val enrichedRating = x.join(y).map {
    case(key, (movie,rating)) => toCsv(rating,movie)
  }
  enrichedRating.take(20).foreach(println)

  val enrichedRatingLeftJoin = x.leftOuterJoin(y).map {
    case(key, (movie, Some(rating))) => toCsv(rating,movie)
    case(key, (movie, None)) => s"We couldn't find rating for movie ${movie.title}"
  }

  enrichedRatingLeftJoin.take(20).foreach(println)

  while(true) Thread.sleep(1000)

  sparkContext.stop()

  def toCsv(rating: Rating, movie: Movie): String = s"${rating.rID},${rating.stars},${movie.title}"

}
