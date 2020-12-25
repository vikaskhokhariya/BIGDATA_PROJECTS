package ca.mcit.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/** This is Driver */
object Spark01CoreRdd extends App with Base {

  println("Started the Driver Program")

  val sparkConf = new SparkConf().setAppName("Spark Core Practice").setMaster("local[*]")
  val sparkContext = new SparkContext(sparkConf)

  val scalaCollection: Seq[Int] = (1 to 1000).map(_ => Random.nextInt())
  val x0: RDD[Int] = sparkContext.parallelize(scalaCollection)
  println(s"Number of Partitions is ${x0.getNumPartitions}")
  x0.foreachPartition(p => println(p.toList.size))

  val x1 = x0.repartition(4)
  println(s"Number of Partitions is ${x1.getNumPartitions}")
  x1.foreachPartition(p => println(p.toList.size))

  while(true) Thread sleep(1000)

  sparkContext.stop()

}
