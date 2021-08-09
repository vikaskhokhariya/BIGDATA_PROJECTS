package sprint2

import scala.io.Source
import org.apache.hadoop.fs.Path
import org.apache.hive.jdbc.HiveDriver
import java.sql.DriverManager
import io.circe._
import io.circe.parser._

object jdbcMainObject extends hClient with App {

  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=mrvikkku;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

  val externalPath = new Path("/user/bdss2001/mrvikkku/external")

  //Check if Directory is exist or not, if exist then Delete
  if (fs.exists(externalPath))
    fs.delete(externalPath, true)

  //Create Directories
  fs.mkdirs(new Path(s"$externalPath"))
  fs.mkdirs(new Path(s"$externalPath"+"/station_information"))
  fs.mkdirs(new Path(s"$externalPath"+"/system_information"))

  //Convert System information into CSV
  val systemInfoJsonFile: String = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/system_information.json").mkString
  val docSystem: Json = parse(systemInfoJsonFile).getOrElse(Json.Null)
  val cursorForSystem: HCursor = docSystem.hcursor
  val systemCursor = cursorForSystem.downField("data")

  val sysInfoString = new StringBuilder("system_id,time_zone\n")
  val systemId = systemCursor.downField("system_id").as[String].getOrElse(Json.Null)
  val timeZone = systemCursor.downField("timezone").as[String].getOrElse(Json.Null)

  val systemInformationFile = fs.create(new Path("/user/bdss2001/mrvikkku/external/system_information/system_information.csv"))
  systemInformationFile.write(sysInfoString.append(systemId + "," + timeZone + "\n").toString().getBytes())
  systemInformationFile.close()


  // Convert Station Information to CSV
  val stationInfoJsonFile: String = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/station_information.json").mkString
  val docStation: Json = parse(stationInfoJsonFile).getOrElse(Json.Null)
  val cursorForStation: HCursor = docStation.hcursor
  val stationArray = cursorForStation.downField("data").downField("stations")
  val stationInfoString = new StringBuilder("station_id, name, short_name, lat, lon, capacity\n")

  for (stationValues <- stationArray.values.get) {
    val stationCursor = stationValues.hcursor

    val stationId = stationCursor.downField("station_id").as[Int].getOrElse(Json.Null)
    val name = stationCursor.downField("name").as[String].getOrElse(Json.Null)
    val shortName = stationCursor.downField("short_name").as[String].getOrElse(Json.Null)
    val lat = stationCursor.downField("lat").as[Double].getOrElse(Json.Null)
    val lon = stationCursor.downField("lon").as[Double].getOrElse(Json.Null)
    val capacity = stationCursor.downField("capacity").as[Int].getOrElse(Json.Null)

    stationInfoString.append(stationId + "," + name + "," + shortName + "," + lat + "," + lon + "," + capacity + "\n")
  }

  val stationInformationFile = fs.create(new Path("/user/bdss2001/mrvikkku/external/station_information/station_information.csv"))
  stationInformationFile.write(stationInfoString.toString().getBytes())
  stationInformationFile.close()

  //Drop the table enriched_trip
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.ext_station_information")
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.ext_system_information")
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.enriched_station_information")

  stmt.execute(
    """create external table bdss2001_mrvikkku.ext_station_information(
      |    station_id int,
      |    external_id string,
      |    name string,
      |    short_name int,
      |    lat double,
      |    lon double,
      |    rental_methods0 string,
      |    rental_methods1 string,
      |    capacity int,
      |    electric_bike_surcharge_waiver boolean,
      |    is_charging boolean,
      |    eightd_has_key_dispenser boolean,
      |    has_kiosk boolean
      |)
      |row format delimited
      |fields terminated by ','
      |stored as textfile
      |location '/user/bdss2001/mrvikkku/external/station_information'
      |tblproperties(
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = "")""".stripMargin)

  stmt.execute(
    """create external table bdss2001_mrvikkku.ext_system_information(
      |   system_id string,
      |   timezone string
      |)
      |row format delimited
      |fields terminated by ','
      |stored as textfile
      |location '/user/bdss2001/mrvikkku/external/system_information'
      |tblproperties(
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = "")""".stripMargin)

  stmt.execute(
    """create table bdss2001_mrvikkku.enriched_station_information(
      |   system_id string,
      |   timezone string,
      |   station_id int,
      |   name string,
      |   short_name string,
      |   lat double,
      |   long double,
      |   capacity int
      |)
      |stored as parquet""".stripMargin)

  //Set Partition mode as nonstrict
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("set hive.mapred.mode=nonstrict")

  //Insert data into enriched_station_information
  stmt.execute(
    """insert into table bdss2001_mrvikkku.enriched_station_information
      |SELECT
      |    sy.system_id,
      |    sy.timezone,
      |    st.station_id,
      |    st.name,
      |    st.short_name,
      |    st.lat,
      |    st.lon,
      |    st.capacity
      |FROM bdss2001_mrvikkku.ext_station_information st
      |    cross join bdss2001_mrvikkku.ext_system_information sy""".stripMargin)
}
