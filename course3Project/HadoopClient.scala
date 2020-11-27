package Project31

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient{
  val conf = new Configuration()
  val hadoopConfDir = "c:/Users/DELL/IdeaProjects/hadoop-playgrounds/data/ClientXmlFile"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  //Create the Client
  val fs = FileSystem.get(conf)
//  fs.delete(new Path("/user/bdss2001/mrvikkku/course3/enrichedTrip.txt"))
}
