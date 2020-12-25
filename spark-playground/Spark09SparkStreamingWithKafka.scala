package ca.mcit.bigdata.spark

import ca.mcit.bigdata.spark.Spark08StreamingTextFile.ssc
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark09SparkStreamingWithKafka extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val sparkConf = new SparkConf().setAppName("Spark streaming practices").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))

  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "kis",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )


  val topic = "stop_times"
  val kafkaStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
  )

  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  // 3. Business logic for each micro-batch (a micro-batch is an RDD)
  // my business logic is to implement word count
  kafkaStreamValues.foreachRDD(rdd => businessLogic(rdd))

  // 4. Start streaming and keep running
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)

  /** Calculate the word count */
  def businessLogic(rdd: RDD[String]): Unit = {
    rdd.flatMap(_.split(" ")).map(w => w -> 1).reduceByKey(_ + _).take(10).foreach {
      case (w, c) => println(s"Word '$w' is repeated $c times")
    }
  }

}
