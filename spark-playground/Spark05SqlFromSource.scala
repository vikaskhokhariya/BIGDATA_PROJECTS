package ca.mcit.bigdata.spark

import org.apache.spark.sql.SparkSession

object Spark05SqlFromSource extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder().appName("SparkSqlFromSource").master("local[*]").getOrCreate()

  val movieDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/movie/movie.csv")
  val ratingDf = spark.read
    .option("header","true").option("inferschema","true")
    .csv("/user/bdss2001/mrvikkku/rating/rating.csv")

  movieDf.printSchema()
  movieDf.show()

  ratingDf.printSchema()
  ratingDf.show()

  spark.stop()

}
