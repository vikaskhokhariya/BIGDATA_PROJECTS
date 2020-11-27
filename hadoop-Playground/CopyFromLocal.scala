package ca.mcit.bigdata.hadoop

import ca.mcit.bigdata.hadoop.Hadoop01.fs
import org.apache.hadoop.fs.{FileStatus, Path}

object CopyFromLocal extends HadoopClient with App {

  //hadoop fs -copyFromLocal /local/file /remote/path
  fs.copyFromLocalFile(new Path("data/STM/agency.txt"), new Path("/user/bdss2001/mrvikkku/"))

  //Delete File
  //fs.delete(new Path("/user/bdss2001/mrvikkku/calendar.txt"),false)
}
 