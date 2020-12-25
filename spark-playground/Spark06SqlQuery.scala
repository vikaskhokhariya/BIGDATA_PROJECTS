package ca.mcit.bigdata.spark

import org.apache.spark.sql.SparkSession

object Spark06SqlQuery extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder().appName("SparkSqlQuery").master("local[*]").getOrCreate()

  val movieDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/movie/movie.csv")
  val ratingDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/rating/rating.csv")

  movieDf.createOrReplaceTempView("movie")
  ratingDf.createOrReplaceTempView("rating")

  val movieRatingAfter2000 = spark.sql(
    """SELECT *
      |FROM movie JOIN rating on movie.mID = rating.mID
      |WHERE movie.year > 2000
      |""".stripMargin
  )

  movieRatingAfter2000.show()

  spark.stop()

}
