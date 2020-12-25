package ca.mcit.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02CoreHdfs extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Core Practice HDFS")
  val sparkContext = new SparkContext(sparkConf)

  val inputRdd = sparkContext.textFile("/user/bdss2001/mrvikkku/movie.csv")
  //val movieRDD: RDD[Movie] = inputRdd.map((item: String) => Movie.apply(item))
  val movieRDD: RDD[Movie] = inputRdd.filter(!_.startsWith("mID"))map(Movie(_))

  val movies: Array[Movie]= movieRDD.take(10)
  movies.foreach(println)

  while(true) Thread.sleep(1000)

  sparkContext.stop()

}
