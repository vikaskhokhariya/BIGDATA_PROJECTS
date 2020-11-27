package ca.mcit.bigdata.hadoop

import org.apache.hadoop.fs.{FileStatus, Path}

object LMS2 extends HadoopClient with App
{

  //1.	Print effective URI
  //val Content = fs.listLocatedStatus(new Path("/user/bdss2001/mrvikkku"))
  //println(Content)

  //2.	Try to list the content of the “/user/yourgroup”.
  val Content1: Array[FileStatus]= fs.listStatus(new Path("/user/bdss2001/"))
  val newContent: List[String] = Content1.map(_.getOwner).toList

  for (name <- newContent) {
       if(name.equals("mrvikkku")) print("I Found my Folder")
  }
}
