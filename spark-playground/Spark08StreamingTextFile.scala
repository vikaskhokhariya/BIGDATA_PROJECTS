package ca.mcit.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark08StreamingTextFile extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val sparkConf = new SparkConf().setAppName("Spark streaming practices").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))

  val fileStream: DStream[String] = ssc.textFileStream("C:\\Users\\DELL\\IdeaProjects\\spark-playground\\data")

  fileStream.foreachRDD(rdd => businessLogic(rdd))

  ssc.start()
  ssc.awaitTermination()

  def businessLogic(rdd: RDD[String]): Unit = {
    rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .take(10).foreach(println)
  }
}
