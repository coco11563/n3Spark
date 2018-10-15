package reduce
import java.io.{File, FileOutputStream, FileWriter}

import Function.{HDFSHelper, regexFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.hdfs.client.HdfsAdmin

import scala.language.postfixOps
import scala.sys.process._
class fileGroup(filePath : String, outpath : String) {
  def output : String = outpath
  def ret : Stream[String] = s"hadoop fs -ls -R $filePath" #| "grep PART"lines_!
  def fl: List[String] = ret.toList

  def entity_status_path : List[(String, String)] = fl
    .withFilter(x => regexFunction.csv_entity_part_regex.matcher(x).find)
    .map(s => {
      val m = regexFunction.csv_entity_part_regex.matcher(s)
      m.find()
      (m.group("fname"), m.group())
    }
    )

  def relationship_status_path: List[(String, String)] = fl
    .withFilter(x => regexFunction.csv_entity_part_regex.matcher(x).find)
    .map(s => {
      val m = regexFunction.csv_entity_part_regex.matcher(s)
      m.find()
      (m.group("fname") + m.group(2), m.group())
    }
    )
  def append(sPair : (String, String)): Unit = {
    val localpath = outpath + sPair._1
    val hdfspath = new Path(sPair._2)
    val recursive = true
    val hdfs : FileSystem = FileSystem.get(new Configuration)
    if(HDFSHelper.exists(hdfs, hdfspath)) {
      val fileList = hdfs.listFiles(hdfspath, recursive)
      while(fileList.hasNext) {
        val fileStatus = fileList.next()
        val name = fileStatus.getPath.getName
        val me = regexFunction.csv_entity_part_regex.matcher(name)
        val mr = regexFunction.csv_relationship_part_regex.matcher(name)
        if (me.find()) { // is entity
          var file_name = me.group("fname")
          var outputPath = outpath + "/" + file_name

        } else if (mr.find()) {// is relationship
          var file_name = mr.group("fname")
          var outputPath = outpath + "/" + file_name

        } else { //is dir

        }
      }
    }
  }
}
