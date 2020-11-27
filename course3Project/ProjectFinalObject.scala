package Project31

import scala.io.Source
import org.apache.hadoop.fs.Path

object ProjectFinalObject extends HadoopClient with App{

  //HDFS Directory Path
  val hdfsDir = "/user/bdss2001/mrvikkku"

  val stmPath = new Path(s"$hdfsDir/STM")
  val course3Path = new Path(s"$hdfsDir/course3")

  //Check if Directories are exist or not, if yes then Delete
  if (fs.exists(stmPath))
    fs.delete(stmPath, true)

  if (fs.exists(course3Path))
    fs.delete(course3Path, true)


  //Create a Directory called as "STM" with three files
  fs.mkdirs(new Path(s"$hdfsDir/STM"))

  fs.copyFromLocalFile(new Path("data/STM/trips.txt"),new Path(s"$hdfsDir/STM/"))
  fs.copyFromLocalFile(new Path("data/STM/routes.txt"),new Path(s"$hdfsDir/STM/"))
  fs.copyFromLocalFile(new Path("data/STM/calendar.txt"),new Path(s"$hdfsDir/STM/"))


  //Create a Directory called as "course3" with one file
  fs.mkdirs(new Path(s"$hdfsDir/course3"))
  val fileName = new Path(s"$hdfsDir/course3/enrichedTrip.txt")
  val createdFile = fs.create(fileName)

  //List of Trip Class
  val streamTrip = fs.open(new Path(s"$hdfsDir/STM/trips.txt"))
  val sourceTrip = Source.fromInputStream(streamTrip)
  val resultTrip: List[String] = sourceTrip.getLines().toList
  val trips: List[Trip] = resultTrip.tail.map(convertLinesToTrip)

  //List of Route Class
  val streamRoute = fs.open(new Path(s"$hdfsDir/STM/routes.txt"))
  val sourceRoute = Source.fromInputStream(streamRoute)
  val resultRoute: List[String] = sourceRoute.getLines().toList
  val routes: List[Route] = resultRoute.tail.map(convertLinesToRoute)

  //List of Calendar Class
  val streamCalendar = fs.open(new Path(s"$hdfsDir/STM/calendar.txt"))
  val sourceCalendar = Source.fromInputStream(streamCalendar)
  val resultCalendar: List[String] = sourceCalendar.getLines().toList
  val calendar: List[Calendar] = resultCalendar.tail.map(convertLinesToCalendar)

  // Object of RoutLookup Class
  val r = new RouteLookup(routes)
  // Object of CalendarLookup Class
  val c = new CalendarLookup(calendar)

  //List Of TripRoute
  val tripRoutes: List[TripRoutes] = trips.map(convertToTripRoute)

  //List of EnrichedTrip
  val enrichedTrip: List[EnrichedTrips] = tripRoutes.map(convertToEnrichedTrip)
  val lines: String = enrichedTrip.map(convertEnrichedTripToFile).mkString("\n")
  //lines.foreach(println)

  val header = "tripId,routeId,serviceId,routeLongName,routeColor,tripHeadSign,monday,tuesday,wednesday\n"
    .concat(lines)

  header.foreach(
    line=> {
      createdFile.writeChars(s"$line")
    }
  )

  def convertLinesToTrip(resultTrips: String) : Trip ={
    val fieldsTrip: Array[String] = resultTrips.split(",",-1)
    val bool: Boolean = if (fieldsTrip(6).toInt == 1) true else false
    Trip(fieldsTrip(0).toInt,fieldsTrip(1),fieldsTrip(2),fieldsTrip(3),bool)
  }

  def convertLinesToRoute(resultRoute: String) : Route ={
    val fieldsRoute: Array[String] = resultRoute.split(",",-1)
    Route(fieldsRoute(0).toInt,fieldsRoute(3),fieldsRoute(6))
  }

  def convertLinesToCalendar(resultCalendar: String) : Calendar ={
    val fieldsCalendar: Array[String] = resultCalendar.split(",",-1)
    Calendar(fieldsCalendar(0), fieldsCalendar(1).toInt, fieldsCalendar(2).toInt,fieldsCalendar(3).toInt,fieldsCalendar(4).toInt,fieldsCalendar(5).toInt,fieldsCalendar(6).toInt,fieldsCalendar(7).toInt,fieldsCalendar(8).toInt,fieldsCalendar(9).toInt)
  }

  def convertToTripRoute(trip: Trip): TripRoutes = {
    val route = r.lookup(trip.routeId)
    TripRoutes(trip,route)
  }

  def convertToEnrichedTrip(tripRoutes: TripRoutes): EnrichedTrips = {
    val calendar = c.lookup(tripRoutes.trip.serviceId)
    EnrichedTrips(tripRoutes,calendar)
  }

  def convertEnrichedTripToFile(enrichedTrip : EnrichedTrips) : String = {
    s"${enrichedTrip.tripRoutes.trip.tripId},${enrichedTrip.tripRoutes.trip.routeId},${enrichedTrip.tripRoutes.trip.serviceId},${enrichedTrip.tripRoutes.route.routeLongName},${enrichedTrip.tripRoutes.route.routeColor},${enrichedTrip.tripRoutes.trip.tripHeadSign},${enrichedTrip.calendar.monday},${enrichedTrip.calendar.tuesday},${enrichedTrip.calendar.wednesday}"
  }

  sourceTrip.close()
  sourceRoute.close()
  sourceCalendar.close()
}




