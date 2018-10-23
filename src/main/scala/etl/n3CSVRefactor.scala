package etl

import Function.regexFunction
import etl.Obj.Entity
import org.apache.spark.{SparkConf, SparkContext}

class n3CSVRefactor {

}
object n3CSVRefactor {
  //重构原因 ： 部分N3文件数量过大，导致无法正常处理（StackOverflow）
  //这个版本会遍历一个文件夹，文件夹内存在若干巨大的n3文件
  //处理这个文件的逻辑与之前类似
  //输入的参数 args(0) -> 处理的文件地址
  //输入的参数 args(1) -> 输出的文件地址
  //输入的参数 args(2) -> 输出的文件名（.csv）
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TestProcess")
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")
      .set("spark.driver.maxResultSize", "4g")

    val sc = new SparkContext(conf)
    val mainFile = args(0)
    val outFile = args(1)
    val outName = args(2)
    val originalRdd = sc.textFile(mainFile)
    println(originalRdd.first())
    val entityRdd = originalRdd
      .filter(s => regexFunction.entity_regex.matcher(s).find() ||
        regexFunction.property_regex.matcher(s).find())
    println(entityRdd.first())
    //取得n3文件中的关系文件
    val relationshipRdd = originalRdd
      .filter(s => regexFunction.rela_regex.matcher(s).find())

    val settleUpEntityRdd = entityRdd.map(s => {
      val me = regexFunction.named_entity_regex.matcher(s)
      val mp = regexFunction.named_property_regex.matcher(s)
      if (me.find()) {
        (me.group("prefix") + me.group("id"), me.group())
      } else {
        mp.find()
        (mp.group("prefix") + mp.group("id"), mp.group())
      }
    }) //RDD(id, str)
      .groupByKey() //main, gather all the same id entity
      .values //get the same id str
      .map(s => s.toList)


    val entitySchema:Seq[String] = settleUpEntityRdd //RDD[List[String]]
      .map(s => s.filter(l => regexFunction.isProperty(l))) //RDD[List[PropStr]]
      .map(s => {
        s.map(line => {
          val m = regexFunction.named_property_regex.matcher(line)
          regexFunction.get(regexFunction.named_property_regex.matcher(line),
            "name")
        })
      }) // RDD[List[KeyStr]]
      .map(l => {
        l.toSet //RDD[Set[KeyStr]]
      })
      .reduce(_ ++ _) //Set[Key]
      .toSeq

//    entitySchema.foreach(s => println(s))

    val entityClassRdd = settleUpEntityRdd.map(l => { //RDD[List[String]]
      val en = l.filter(s => regexFunction.isEntity(s)).head //Entity Str
      val label = regexFunction.get(regexFunction.named_entity_regex.matcher(en),"label") //label
      //prefix + id
      val id = regexFunction.get(regexFunction.named_entity_regex.matcher(en),"prefix")+
        regexFunction.get(regexFunction.named_entity_regex.matcher(en),"id")
      val prop = l
        .filter(s => regexFunction.isProperty(s))//List[PropStr]
        .map(s => ( //may cause the duplicated key/value overwrite
          regexFunction.get(regexFunction.named_property_regex.matcher(s),"name")
            ->
            regexFunction.get(regexFunction.named_property_regex.matcher(s),"value").replace("\"", "\"\"")
          )
        )//(key, value)
        .groupBy(i => i._1)
        .map(f => (f._1, f._2.map(_._2).toArray))
        .toArray
        .toMap
      new Entity(id, label, prop, entitySchema)
    })

//    println(entityClassRdd.first().propSeq.length) // 2

    val cacuArrayEntity = entityClassRdd
      .map(e => e.propSeq.map(_.contains(";")).toSeq)
      .reduce(
        (a,b) => {
        for (i <- a.indices) yield {
          a(i) || b(i)
        }
    })

    val entityCollectionRdd = entityClassRdd.map(_.toString)
    var entitySchemaDemo : Array[String] = Array("ENTITY_ID:ID")
    entitySchemaDemo ++= entitySchema
    entitySchemaDemo :+= "ENTITY_TYPE:LABEL"

//    println(cacuArrayEntity.length)
//    println(entitySchemaDemo.length)

    entitySchemaDemo.foreach(println(_))

    //IndexOutBound
    val finalEntitySchemaDemo =
      for (i <- entitySchemaDemo.indices) yield {
        if (cacuArrayEntity(i))
          entitySchemaDemo(i) + ":String[]"
        else
          entitySchemaDemo(i)
    }

    var final_entity_schema = finalEntitySchemaDemo.reduce((a, b) => a + "," + b)

//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println(final_entity_schema)
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")
//    println("==========================================")

    val entitySchemaRdd = sc.parallelize(Array(final_entity_schema)) //only way to make rdd
    val entityCollectionRddWithHead = entitySchemaRdd ++ entityCollectionRdd

    entityCollectionRddWithHead.saveAsTextFile(outFile + "/entity/" + outName + ".csv")

    val finalRelationshipSchema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE"

    val relationshipCollectionRdd = relationshipRdd.map(s => {
      regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"prefix1") +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id1") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"tprefix") +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"prefix2") +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id2") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type")
    })

    val relationshipCollectionArray = sc.parallelize(Array(finalRelationshipSchema)) //only way to make rdd
    val relationshipCollectionArrayRdd = relationshipCollectionArray ++ relationshipCollectionRdd
    relationshipCollectionArrayRdd
      .saveAsTextFile(outFile + "/relationship/" + outName + ".csv")
  }
}
