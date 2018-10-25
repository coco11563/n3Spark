package etl

import Function.regexFunction
import etl.Obj.Entity
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._
import breeze.linalg._

import scala.language.postfixOps

class n3CSVRefactor {

}
object n3CSVRefactor {
  //重构原因 ： 部分N3文件数量过大，导致无法正常处理（StackOverflow）
  //输入的参数 args(0) -> 处理的文件地址
  //输入的参数 args(1) -> 输出的文件地址
  //输入的参数 args(2) -> 输出的文件名（.csv）
  //一次处理单个N3文件
  def main(args: Array[String]): Unit = {


    val mainFile = args(0)
    val outFile = args(1)
    val outName = args(2)
    val deletePastRelationship = s"hadoop fs -rm -r $outFile$outName" +
      "_relationship"!

    if (deletePastRelationship == 0) println("done delete past relationship with code " + deletePastRelationship)
    else println("wrong with delete past relationship with error code " + deletePastRelationship)

    val deletePastEntity = s"hadoop fs -rm -r $outFile$outName" +
      "_entity"!

    if (deletePastRelationship == 0) println("done delete past entity with code " + deletePastEntity)
    else println("wrong with delete past entity with error code " + deletePastEntity)


    val conf = new SparkConf()
      .setAppName("ProcessOn" + outName)
      .set("spark.neo4j.bolt.url","bolt://neo4j:1234@10.0.88.50")
      .set("spark.driver.maxResultSize", "4g")

    val sc = new SparkContext(conf)
    val originalRdd = sc.textFile(mainFile)
    println(originalRdd.first())
    val entityRdd = originalRdd
      .filter(s => regexFunction.named_entity_regex.matcher(s).find() ||
        regexFunction.named_property_regex.matcher(s).find())
    println(entityRdd.first())
    //取得n3文件中的关系文件
    val relationshipRdd = originalRdd
      .filter(s => regexFunction.named_property_regex.matcher(s).find())

    val settleUpEntityRdd = entityRdd.map(s => {
      val me = regexFunction.named_entity_regex.matcher(s)
      val mp = regexFunction.named_property_regex.matcher(s)
      if (me.find()) {
        (me.group("id"), me.group())
      } else {
        mp.find()
        (mp.group("id"), mp.group())
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
          m.find()
          val flag = m.group("flag")
          val value = m.group("value")
          val name = m.group("name")
          if (value == flag) flag else name
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
      //id
      val id = regexFunction.get(regexFunction.named_entity_regex.matcher(en),"id")
      val prop = l
        .filter(s => regexFunction.isProperty(s))//List[PropStr]
        .map(s => //may cause the duplicated key/value overwrite
      {
        val m = regexFunction.named_property_regex.matcher(s)
        m.find()
        val flag = m.group("flag")
        val flagName = m.group("flagname")
        val value = m.group("value")
        val name = m.group("name")
        if (value == flag) flag -> flagName else name -> value
      }
      )//(key, value)
        .groupBy(i => i._1) //mk the same value to same group then the duplicated one could store in an ARRAY
        .map(f => (f._1, f._2.map(_._2).toArray))
        .toArray
        .toMap
      new Entity(id, label, prop, entitySchema) // get the new entity
    })

    //    println(entityClassRdd.first().propSeq.length) // 2

    val cacuArrayEntity = entityClassRdd
      .map(e => DenseVector(e.propSeq.map(_.contains(";")))) //the ';' will only show when the Str is Array
      .reduce( // if this one is duplicated key one then the return will be true
        _ :| _ //using breeze lib to replace the yield operation
    )

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
          entitySchemaDemo(i) + ":String[]"  // this one will use the TRUE/FALSE list to generate the right schema with SchemaType
        else
          entitySchemaDemo(i) // in this case the element is not duplicated one, so the schema will not followed by ":String[]"
    }

    var final_entity_schema = finalEntitySchemaDemo.reduce((a, b) => a + "," + b) // get the final schema, finish the entity schema generate

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
    val entityCollectionRddWithHead = entitySchemaRdd ++ entityCollectionRdd // gear up the csv with it's head

    entityCollectionRddWithHead.saveAsTextFile(outFile  + outName +"_entity") // store the csv file

    val finalRelationshipSchema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE" // make the relationship schema

    val relationshipCollectionRdd = relationshipRdd.map(s => { // generate the relationship rdd
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id1") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id2") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type")
    })

    val relationshipCollectionArray = sc.parallelize(Array(finalRelationshipSchema)) //only way to make rdd
    val relationshipCollectionArrayRdd = relationshipCollectionArray ++ relationshipCollectionRdd // build the relationship csv with head
    relationshipCollectionArrayRdd
      .saveAsTextFile(outFile + outName + "_relationship") // store the relationship rdd file



    val deletePastMergedRelationship = s"hadoop fs -rm -r $outFile$outName" +
      s"_relationship.csv"!

    if (deletePastMergedRelationship == 0) println("relationship merge delete done with " + deletePastMergedRelationship)
    else println("wrong with relationship merge delete with error code " + deletePastMergedRelationship)

    val deletePastMergedEntity = s"hadoop fs -rm -r $outFile$outName" +
      s"_relationship.csv"!

    if (deletePastMergedEntity == 0) println("entity merge delete done with " + deletePastMergedEntity)
    else println("wrong with entity merge delete with error code " + deletePastMergedEntity)


    val mergeEntity = s"hadoop fs -cat ${outFile + outName}_entity/*" #| s"hadoop fs -put - ${outFile + outName}_entity.csv"!

    if (mergeEntity == 0) println("entity merge done with " + mergeEntity)
    else println("wrong with entity merge with error code " + mergeEntity)

    val mergeRelationship = s"hadoop fs -cat ${outFile + outName}_relationship/*" #| s"hadoop fs -put - ${outFile + outName}_relationship.csv"!

    if (mergeRelationship == 0) println("relationship merge done with " + mergeRelationship)
    else println("wrong with relationship merge with error code " + mergeRelationship)
  }
}
