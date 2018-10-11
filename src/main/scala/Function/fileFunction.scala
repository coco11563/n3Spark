package Function
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import scala.language.postfixOps
import scala.sys.process._


class fileFunction (filepath : String) {
  def ret : Stream[String] = s"hadoop fs -ls -R $filepath" #| "grep .n3"lines_!
  def file_path: List[String] = ret
    .toList
    .withFilter(x => regexFunction.file_regex.matcher(x).find)
    .map(s => {
      val m = regexFunction.file_regex.matcher(s)
      m.find()
      m.group()
    })
  def status_path : List[(String, String)] = ret.toList
    .withFilter(x => regexFunction.file_regex.matcher(x).find)
    .map(s => {
      val m = regexFunction.file_regex.matcher(s)
      m.find()
      (m.group(1) + m.group(2), m.group())
    }
    )
}

object fileFunction{
  def main (args: Array[String] ): Unit = {
//    val f = new fileFunction("/data/alldataNew")
//    println(f.file_path)
//    println(checkPath("/data/alldataNew"))
  }

}
