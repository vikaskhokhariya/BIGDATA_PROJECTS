package ca.mcit.bigdata.spark

case class Movie(mID: Int,title: String,year: Int,director: Option[String])

object Movie {

  def apply(csv: String): Movie = {
    val fields = csv.split(",", -1)
    Movie(fields(0).toInt, fields(1), fields(2).toInt, if (fields.length==4) Option(fields(3)) else None)
  }

}
