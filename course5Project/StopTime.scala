package project5

case class StopTime(
   trip_Id: String,
   arrival_time: String,
   departure_time: String,
   stop_id: String,
   stop_sequence: String)

object StopTime
{
  def apply(csv: String) : StopTime = {
    val fields = csv.split(",")
    StopTime(fields(0), fields(1), fields(2), fields(3), fields(4))
  }

}
