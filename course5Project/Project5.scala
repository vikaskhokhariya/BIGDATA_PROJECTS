package project5

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.BooleanType

object Project5 extends App with Base {

  System.setProperty("hadoop.home.dir", "C:\\winutils")
  val hdfsPath = "/user/bdss2001/mrvikkku"

  val spark = SparkSession.builder().appName("Project").master("local[*]").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

  /* ~~~~~~~~~~ Create DataFrames ~~~~~~~~~~ */
  var tripsDf: DataFrame = spark.read
    .option("header","true").option("inferschema","true")
    .csv(s"$hdfsPath/project5/trips/trips.txt")
  val routesDf: DataFrame = spark.read
    .option("header","true").option("inferschema","true")
    .csv(s"$hdfsPath/project5/routes/routes.txt")
  val calendarDateDf: DataFrame = spark.read
    .option("header","true").option("inferschema","true")
    .csv(s"$hdfsPath/project5/calendar_dates/calendarDates.txt")

  tripsDf = tripsDf
    .withColumn("wheelchair_accessible", col("wheelchair_accessible")
      .cast(BooleanType))

  /* ~~~~~~~~~~ Create TempView ~~~~~~~~~~ */
  tripsDf.createOrReplaceTempView("trips")
  calendarDateDf.createOrReplaceTempView("calendarDates")
  routesDf.createOrReplaceTempView("routes")

  /* ~~~~~~~~~~ Enrichment of Route,Trip and Calender Date ~~~~~~~~~~ */
  val enrichedTripDf: DataFrame = spark.sql(
    """SELECT t.trip_id,t.service_id,r.route_id,t.trip_headsign,t.wheelchair_accessible,c.date,c.exception_type,r.route_long_name,r.route_color
      |FROM routes r
      |left JOIN trips t ON r.route_id = t.route_id
      |left JOIN calendarDates c ON c.service_id = t.service_id""".stripMargin)

  /* ~~~~~~~~~~ Stream Stop Time ~~~~~~~~~~ */
  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "kis",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val topicName = "stoptime"
  val kafkaStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topicName), kafkaConfig))

  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  kafkaStreamValues.foreachRDD(stopTimesRdd => processAndEnrichment(stopTimesRdd))

  /* ~~~~~~~~~~ Enrichment of EnrichedTrip and StopTime ~~~~~~~~~~ */
  import spark.implicits._
  def processAndEnrichment(stopTimesRdd: RDD[String]): Unit = {
    val stopTimeDF: DataFrame = stopTimesRdd.map(StopTime(_)).toDF
    stopTimeDF.join(enrichedTripDf, "trip_id")
      .write.mode(SaveMode.Append).json(s"$hdfsPath/project5/enriched_stop_time")
  }

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)
  spark.close()

}
