package ca.mcit.bigdata.spark

case class Rating(rID: Int, mID: Int ,stars: Int, ratingDate: Option[String])

object Rating {

  def apply(csv: String) : Rating = {
    val fields = csv.split(",")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, if (fields.length==4) Option(fields(3)) else None)
  }

}
