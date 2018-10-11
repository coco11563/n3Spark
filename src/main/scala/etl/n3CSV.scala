package etl

import java.io.StringWriter

import Function.{fileFunction, regexFunction}
import etl.Obj.Entity
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

class n3CSV {

}
object n3CSV {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("TestProcess")
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")

    val sc = new SparkContext(conf)
    val main_files = new fileFunction(args(0))
//    main_files.status_path.foreach(a => println(a._1 + " , " + a._2))
    val files_group_by_files = sc
      .parallelize(main_files.status_path)
      .groupByKey()
      .collect()
      .map(s => (s._1, s._2.toList))
//    files_group_by_files.foreach(l => println(l.reduce((a,b) => a + "," + b)))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //main body
    for (l <- files_group_by_files) {
      val temp_rdd_list = for (s <- l._2) yield {
        sc.textFile(s)
      }
      //一个目录下的n3文件
      val original_rdd: RDD[String] =
        temp_rdd_list.reduce(_ union _)
        .persist(StorageLevel.MEMORY_AND_DISK)
//      l.foreach(s => println(s))
//      original_rdd.repartition(30)
      //取得n3文件中的实体与属性
//      val count_1 = original_rdd.filter(s => regexFunction.entity_regex.matcher(s).find()).count()
      val entity_rdd = original_rdd
        .filter(s => regexFunction.entity_regex.matcher(s).find() ||
          regexFunction.property_regex.matcher(s).find())
      //取得n3文件中的关系文件
      val relationship_rdd = original_rdd
        .filter(s => regexFunction.rela_regex.matcher(s).find())
      //id, lines[]
      val set_up_entity_rdd = entity_rdd.map(s => {
        val me = regexFunction.named_entity_regex.matcher(s)
        val mp = regexFunction.named_property_regex.matcher(s)
        if (me.find()) {
          (me.group("prefix") + me.group("id"), me.group())
        } else {
          mp.find()
          (mp.group("prefix") + mp.group("id"), mp.group())
        }
      })
        .groupByKey()
        .values
        .map(s => s.toList)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val entity_schema:Seq[String] = set_up_entity_rdd
        .map(s => s.filter(l => regexFunction.isProperty(l)))
        .map(s => {
          for (line <- s) yield {
            val m = regexFunction.named_property_regex.matcher(line)
            regexFunction.get(regexFunction.named_property_regex.matcher(line),
                "name")
          }
        })
        .map(l => {
          l.toSet
        })
        .reduce(_ ++ _)
        .toSeq

      import com.opencsv.CSVWriter
      val entity_collection_rdd = set_up_entity_rdd.map(l => {
        val en = l.filter(s => regexFunction.isEntity(s)).head
        val label = regexFunction.get(regexFunction.named_entity_regex.matcher(en),"lprefix") + regexFunction.get(regexFunction.named_entity_regex.matcher(en),"label")
        val id = regexFunction.get(regexFunction.named_entity_regex.matcher(en),"prefix") + regexFunction.get(regexFunction.named_entity_regex.matcher(en),"id")
        val prop = l
          .filter(s => regexFunction.isProperty(s))
          .map(s => (
              regexFunction.get(regexFunction.named_property_regex.matcher(s),"name")
            ->
              regexFunction.get(regexFunction.named_property_regex.matcher(s),"value")
            )
          ).toArray
          .toMap
        new Entity(id, label, prop, entity_schema)
      })
        .map(e => e.propSeq)
//        .repartition(20)
        .map(iter => {
          val stringWriter = new StringWriter()
          val csvWriter = new CSVWriter(stringWriter)
          csvWriter.writeNext(iter)
          stringWriter.toString
        })

      val final_entity_schema = "ENTITY_ID:ID," + entity_schema.reduce((a, b) => a + "," + b) + ",ENTITY_TYPE:LABEL"
      val entity_collection_array = Array(final_entity_schema) ++ entity_collection_rdd.collect()
//      val count_2 = entity_collection_rdd.count()
      //输出该实体的csv
      sc.parallelize(entity_collection_array,1).saveAsTextFile(args(1) + "/entity/" + l._1)

      val final_relationship_schema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE"

      val relationship_collection_rdd = relationship_rdd.map(s => {
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"prefix1") +
          regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id1") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"tprefix") +
          regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"prefix2") +
          regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id2") + "," +
          regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type")
      })

      val relationship_collection_array = Array(final_relationship_schema) ++ relationship_collection_rdd.collect()
      //输出实体对应的关系
      sc.parallelize(relationship_collection_array,1).saveAsTextFile(args(1) + "/relationship/" + l._1 )
    }
  }
}
