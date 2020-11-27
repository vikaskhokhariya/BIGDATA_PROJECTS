package ca.mcit.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object Hadoop01 extends App {

  //Set the configuration
  val conf = new Configuration()
  val hadoopConfDir = "c:/Users/DELL/IdeaProjects/hadoop-playgrounds/data/ClientXmlFile"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  //Create the Client
  val fs = FileSystem.get(conf)

  //send command to HDFS : fs -ls /
  val content : Array[FileStatus] = fs.listStatus(new Path("/"))
  content.map(_.getPath).foreach(println)

  //fs.mkdirs(new Path("/user/bdss2001/mrvikkku/hellovikas"))

  //close the file system to free resources
  fs.close()
}
