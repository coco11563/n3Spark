package reduce
import java.io.File

import Function.{HDFSHelper, regexFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
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
    val hdfspath = sPair._2

    val hdfs : FileSystem = FileSystem.get(new Configuration)
    if(HDFSHelper.exists(hdfs, hdfspath)) {
//      hdfs.copyToLocalFile()
    }
  }
}
