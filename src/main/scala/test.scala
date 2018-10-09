import java.util.regex.{Matcher, Pattern}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.sys.process._

class test {

}
//private val rela_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:[^#][^t][^y][^p][^e])(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)"
//private val property_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\")([^\"]+)(?:\") (?:\\.)"
//private val entity_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:#type)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)"

object test{

  def main(args: Array[String]): Unit = {
//    val rela_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)([^\\/>]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)((?:[^\\/>]+)(?:[^#])(?:[^t])(?:[^y])(?:[^p])(?:[^e]))(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/>]+)(?:>) (?:\\.\\n)"
//    val property_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)(?:[^\\/>]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)(?:[^\\/>]+)(?:>) (?:\")(?:[^\"]+)(?:\") (?:\\.\\n)"
//    val entity_regex = "(?:<)(?:http:\\/\\/[^>]+\\/)([^\\/>]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/>]+)(?:#type)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/>]+)(?:>) (?:\\.\\n)"
//
//    val rela_pattern = Pattern.compile(rela_regex)
//    val pro_pattern = Pattern.compile(property_regex)
//    val entity_pattern = Pattern.compile(entity_regex)
    val script = "hadoop fs -lsr /data/alldataNew | grep .n3"
    val ret : Stream[String] = "hadoop fs -lsr /data/alldataNew" #| "grep .n3"lines_!
//    val accu : Accumulator[Int] = new Accumulator(0)
    val conf = new SparkConf()
      .setAppName("TestProcess")
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")
    var sc = new SparkContext(conf)
    var final_rdd = sc.emptyRDD[(Int,Int)]
    ret.foreach(file => {
      val m = file_regex.matcher(file)
      if (m.find()) {
        val filename = m.group()
        var text_rdd = sc.textFile(filename)
        println(">>>>>>>>>>>>>>>>>>>>开始处理 : " + filename)
//        println("get all count is " + text_rdd.count())
        var e_rdd = text_rdd.filter(s => isProperty(s) || isEntity(s))//属性和实体丢一起
        var r_rdd = text_rdd.filter(s => isRelationship(s)) //关系放一起
        final_rdd.union(e_rdd.map(s => {
          var me = entity_regex.matcher(s)
          var mp = property_regex.matcher(s)
          var s1 = get(mp, 2)
          if (s1 == "") s1 = get(me, 2)
          (s1, s)
        }).groupByKey()
          .mapValues(i => {
            i.filter(s => isProperty(s))
              .toList
              .map(s => {
                var mp = property_regex.matcher(s)
                get(mp, 4)
              })
              .map(i => (i, 1))
          })
          .mapValues(a => {
            var b = new mutable.HashSet[String]()
            a.foreach(s => {
              b.add(s._1)
            })
            b.size
          })
          .values
          .map(i => (i, 1))
          .reduceByKey((a, b) => {a + b}))
//          .saveAsTextFile("/data/out/" + m.group(1) + m.group(2) + ".out")
      }
    })
    final_rdd.reduceByKey((i,j) => i + j).saveAsTextFile("/data/out/")
  }

  def get(m: Matcher, index: Int): String = {
    if (m.find) m.group(index)
    else ""
  }
  //#1 http type_1 #2 id_1 #3 name #4 http type_2 #5 id_2
  private val rela_regex = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:[^#][^t][^y][^p][^e])(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)")
  //#1 http type #2 id #3 name #4 value
  private val property_regex = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\")(.+)(?:\") (?:\\.)")
  //#1 http type #2 id #3 label
  private val entity_regex = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:#type)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)")
  //#1 type #2 number
  private val file_regex = Pattern.compile("(?:\\/data\\/alldataNew\\/)(\\w+)(?:\\/)(\\w+)(?:\\.n3)")

  def isProperty(str : String) : Boolean = {
    property_regex.matcher(str).find()
  }

  def isRelationship(str : String) : Boolean = {
    rela_regex.matcher(str).find()
  }

  def isEntity(str : String) : Boolean = {
    entity_regex.matcher(str).find()
  }

  def getValue(p : Pattern, str:String, index: Int) : String = {
    p.matcher(str).group(index)
  }

}
