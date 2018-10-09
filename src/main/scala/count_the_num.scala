import Function.regexFunction
import org.apache.spark.{SparkConf, SparkContext}
import test._

import scala.collection.mutable
import scala.sys.process._

class count_the_num {

}
object count_the_num {
  def main(args: Array[String]): Unit = {
    val script = "hadoop fs -lsr /data/alldataNew | grep .n3"
    val ret : Stream[String] = "hadoop fs -lsr /data/alldataNew" #| "grep .n3"lines_!
    //    val accu : Accumulator[Int] = new Accumulator(0)
    val conf = new SparkConf()
      .setAppName("TestProcess")
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")
    var sc = new SparkContext(conf)
    var count = for (s <- ret) yield {
      val m = regexFunction.file_regex.matcher(s)
      if (m.find()) {
        val filename = m.group()
        sc.textFile(filename)
          .count()
      } else {
        0l
      }
    }
    var c = 0l
    for (i <- count) {
      c += i
    }
    println("===============================================")

    println("===============================================")

    println("===============================================")

    println("===============================================")

    println("==============the final ans is " + c + "===========")

    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")

    println("===============================================")

    println("===============================================")

    println("===============================================")

    println("===============================================")

    println("==============the final file count is " + ret.size + "===========")

    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
    println("===============================================")
  }
}
