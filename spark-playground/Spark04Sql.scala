package ca.mcit.bigdata.spark

import ca.mcit.bigdata.spark.Spark03CoreHdfs.{inputRddForMovie, sparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object Spark04Sql extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")
  /*
  * Spark core => only sparkContext
  * Spark sql => Spark Sql wraps a SparkContext
  * SparkContext + SqlContext
  * */

  val spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate()

  val inputRddForMovie: RDD[String] = spark.sparkContext.textFile("/user/bdss2001/mrvikkku/movie.csv")
  val movieRdd: RDD[Movie] = inputRddForMovie.filter(!_.startsWith("mID"))map(Movie(_))

  import spark.implicits._

  /*----------Infer Schema------------*/
  val movieDf : DataFrame = movieRdd.toDF
  movieDf.printSchema()
  movieDf.show()

  /*-----------Program----------------*/
  val movieRdd2: RDD[Row] = inputRddForMovie
    .filter(!_.startsWith("mID"))
    .map(_.split(","))
    .map(csv => Row(csv(0).toInt, csv(1), csv(2).toInt, if(csv.length==4) csv(3) else null))

  val schema = StructType(List(
    StructField("mID", IntegerType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("year", IntegerType, nullable = false),
    StructField("director", StringType, nullable = true)
  ))

  val movieDf2 : DataFrame = spark.createDataFrame(movieRdd2,schema)
  movieDf2.printSchema()
  movieDf2.show()

  spark.close()

}
