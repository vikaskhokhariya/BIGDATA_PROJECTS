package ca.mcit.bigdata.kafka

import ca.mcit.bigdata.kafka.Movie.toCsv
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties

object ProducerPlayground1 extends App {

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

  movieGenerator().foreach{movie =>
    producer.send(
      new ProducerRecord[Int,String]("movie",movie.mID,toCsv(movie))
    )
  }

  producer.flush()
  producer.close()

  def movieGenerator() : List[Movie] = {
    List(
      Movie(103,"kick",2000,"Rohit Shetty"),
      Movie(104,"Bajarangi",2001,"Karan Johar")
    )
  }

}

case class Movie(mID: Int, title: String, year: Int, director: String)
object Movie {
  def toCsv(movie: Movie) : String =
    s"${movie.mID},${movie.title},${movie.year},${movie.director}"
}
