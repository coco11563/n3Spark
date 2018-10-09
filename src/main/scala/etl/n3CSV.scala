package etl

import Function.{fileFunction, regexFunction}
import org.apache.spark.{SparkConf, SparkContext}

class n3CSV {

}
object n3CSV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TestProcess")
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")
    var sc = new SparkContext(conf)
    val main_files = new fileFunction(args(0))
    main_files.file_path.foreach(main_file => {
      //获取n3文件
      val ori_rdd = sc.textFile(main_file)
      //取得n3文件中的实体与属性
      val entity_rdd = ori_rdd.filter(s => regexFunction.entity_regex.matcher(s).find() || regexFunction.property_regex.matcher(s).find())
      //取得n3文件中的关系文件
      val relationship_rdd = ori_rdd.filter(s => regexFunction.rela_regex.matcher(s).find())


    })
  }
}
