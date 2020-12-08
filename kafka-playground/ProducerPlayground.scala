package ca.mcit.bigdata.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties

object ProducerPlayground extends App {

  val producerProperties = new Properties()
  producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[IntegerSerializer].getName
  )
  producerProperties.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer].getName
  )

  val producer = new KafkaProducer[Int,String](producerProperties)

  producer.send(
    new ProducerRecord[Int,String]("movie",101,"Hello Vikas")
  )

  producer.send(
    new ProducerRecord[Int,String]("movie",102,"Hello Akhilesh")
  )

  producer.flush()
  producer.close()

}
