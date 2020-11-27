package course4Project

import java.sql.DriverManager

import org.apache.hadoop.fs.Path
import org.apache.hive.jdbc.HiveDriver

object method2 extends hClient with App {

  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=mrvikkku;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

  val course4Path = new Path("/user/bdss2001/mrvikkku/course4")

  //Check if Directories are exist or not, if yes then Delete
  if (fs.exists(course4Path))
    fs.delete(course4Path, true)

  //Create a Directory called as "course4" with one file
  fs.mkdirs(new Path(s"$course4Path"))

  fs.copyFromLocalFile(new Path("data/routes"), new Path(s"$course4Path"))
  fs.copyFromLocalFile(new Path("data/trips"), new Path(s"$course4Path"))
  fs.copyFromLocalFile(new Path("data/calendarDates"), new Path(s"$course4Path"))

  //Drop the table enriched_trip
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.ext_routes")
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.ext_trips")
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.ext_calendardates")
  stmt.execute("drop table IF EXISTS bdss2001_mrvikkku.enriched_trip")

  //create Schema of ext_routes
  stmt.execute(
    """create external table bdss2001_mrvikkku.ext_routes(
      |    route_id INT,
      |    agency_id STRING,
      |    route_short_name INT,
      |    route_long_name STRING,
      |    route_type INT,
      |    route_url STRING,
      |    route_color STRING,
      |    route_text_color STRING
      |)
      |row format delimited
      |fields terminated by ','
      |stored as textfile
      |location '/user/bdss2001/mrvikkku/course4/routes'
      |tblproperties(
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = "")""".stripMargin)

  //Create Schema of ext_trips
  stmt.execute(
    """create external table bdss2001_mrvikkku.ext_trips(
      |   route_id INT,
      |    service_id STRING,
      |    trip_id STRING,
      |    trip_headsign STRING,
      |    direction_id INT,
      |    shape_id INT,
      |    wheelchair_accessible INT,
      |    note_fr STRING,
      |    note_en STRING
      |)
      |row format delimited
      |fields terminated by ','
      |stored as textfile
      |location '/user/bdss2001/mrvikkku/course4/trips'
      |tblproperties(
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = "")""".stripMargin)

  //Create Schema of ext_calendarDates
  stmt.execute(
    """create external table bdss2001_mrvikkku.ext_calendarDates(
      |   service_id string,
      |   date string,
      |   exception_type int
      |)
      |row format delimited
      |fields terminated by ','
      |stored as textfile
      |location '/user/bdss2001/mrvikkku/course4/calendarDates'
      |tblproperties(
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = "")""".stripMargin)

  stmt.execute(
    """create table if not exists bdss2001_mrvikkku.enriched_trip(
      |    trip_id string,
      |    service_id string,
      |    route_id int,
      |    trip_headsign string,
      |    date string,
      |    exception_type int,
      |    route_long_name string,
      |    route_color string
      |)
      |partitioned by (
      |    wheelchair_accessible int
      |)
      |stored as parquet
      |tblproperties("parquet.compression" = "gzip")""".stripMargin)

  //Set Partition mode as nonstrict
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("set hive.mapred.mode=nonstrict")
  
  //Insert data into enriched_trip
  stmt.execute(
    """insert overwrite table bdss2001_mrvikkku.enriched_trip partition(wheelchair_accessible)
      |SELECT
      |    t.trip_id,
      |    t.service_id,
      |    r.route_id,
      |    t.trip_headsign,
      |    c.date,
      |    c.exception_type,
      |    r.route_long_name,
      |    r.route_color,
      |    t.wheelchair_accessible
      |FROM bdss2001_mrvikkku.ext_routes r
      |    join bdss2001_mrvikkku.ext_trips t on r.route_id = t.route_id
      |    join bdss2001_mrvikkku.ext_calendardates c on t.service_id= c.service_id """.stripMargin)

  stmt.execute("msck repair table bdss2001_mrvikkku.enriched_trip")

  stmt.close()
  connection.close()
  fs.close()
}
