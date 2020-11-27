package ca.mcit.bigdata.hadoop

import org.apache.hadoop.fs.Path

object WriteIntoFile extends HadoopClient with App {

  val names = List("Vikas","Vikkku")
  val fileName = new Path("/user/bdss2001/mrvikkku/names.txt")
  val outputStream = fs.create(fileName)
  names.foreach(name => {
    outputStream.writeChars(s"$name\n")
  })
  outputStream.flush()
  outputStream.close()

}
