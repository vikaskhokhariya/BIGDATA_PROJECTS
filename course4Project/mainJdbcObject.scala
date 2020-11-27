package course4Project

import java.sql.DriverManager

import org.apache.hive.jdbc.HiveDriver

object mainJdbcObject extends App {
  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=mrvikkku;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

  //Drop the table enriched_trip
  stmt.execute("drop table bdss2001_mrvikkku.enriched_trip")

  //Create the table enriched_trip
  stmt.execute(
    """create table if not exists bdss2001_mrvikkku.enriched_trip(
      |    route_id int,
      |    service_id string,
      |    trip_id string,
      |    trip_headsign string,
      |    direction_id int,
      |    shape_id int,
      |    date string,
      |    exception_type int,
      |    route_url string,
      |    route_color string
      |)
      |partitioned by (
      |    wheelchair_accessible int
      |)
      |stored as parquet
      |tblproperties("parquet.compression" = "gzip")""".stripMargin)

  //Set Partition mode as nonstrict
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("set hive.mapred.mode=nostrict")

  //Insert data into enriched_trip
  stmt.execute(
    """insert into bdss2001_mrvikkku.enriched_trip partition(wheelchair_accessible)
                 |SELECT
                 |    r.route_id,
                 |    t.service_id,
                 |    t.trip_id,
                 |    t.trip_headsign,
                 |    t.direction_id,
                 |    t.shape_id,
                 |    c.date,
                 |    c.exception_type,
                 |    r.route_url,
                 |    r.route_color,
                 |    t.wheelchair_accessible
                 |FROM bdss2001_mrvikkku.ext_routes r
                 |    join bdss2001_mrvikkku.ext_trips t on r.route_id = t.route_id
                 |    join bdss2001_mrvikkku.ext_calendardates c on t.service_id= c.service_id """.stripMargin)

  //print data
  val resultSet = stmt.executeQuery("select * from bdss2001_mrvikkku.enriched_trip limit 10")

  while (resultSet.next()) {
    println(s"Movie ID : ${resultSet.getString(1)} \t Name : ${resultSet.getString(2)}")
  }

  stmt.close()
  connection.close()
}
