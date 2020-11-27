package Project31

import org.apache.hadoop.fs.Path

object moveFile extends HadoopClient with App {

  //fs.mkdirs(new Path("/user/bdss2001/mrvikkku/course4"))
  fs.copyFromLocalFile(new Path("C:\\Users\\DELL\\Downloads\\routes"), new Path("/user/bdss2001/mrvikkku/course4"))
  fs.copyFromLocalFile(new Path("C:\\Users\\DELL\\Downloads\\trips"), new Path("/user/bdss2001/mrvikkku/course4"))
  fs.copyFromLocalFile(new Path("C:\\Users\\DELL\\Downloads\\calendarDates"), new Path("/user/bdss2001/mrvikkku/course4"))
  //fs.delete(new Path("/user/bdss2001/mrvikkku/routes"))
  //fs.delete(new Path("/user/bdss2001/mrvikkku/trips"))
  //fs.delete(new Path("/user/bdss2001/mrvikkku/calendarDates"))

}
