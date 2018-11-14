package etl

import Function.regexFunction
import etl.Obj.Entity
import etl.n3CSVBigFileRefactor.removeFileByShell
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._

class n3CSVDFRefactor {

}
object n3CSVDFRefactor {
  def main(args: Array[String]): Unit = {
    val mainFilepath = args(0)
    val outName = args(1)
    //    val countPer = Integer.parseInt(args(2))
    val outFile = "/data/out/temp/"
    val tempMergePath = "/data/out/merge/"
    val mergePath = "/data/out/csv/mergeCSV/"

    val sparkConf = new SparkConf()
      .setAppName("ProcessOn" + outName)
    //      .set("spark.driver.maxResultSize", "4g")
      .set("spark.yarn.driver.memoryOverhead","4096")
      .set("spark.yarn.executor.memoryOverhead","4096")

    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("/tmp/chkpt/")
    process(mainFilepath, outFile, outName, sc, 1, tempMergePath)
  }

  /**
    * outFile + outName + "_relationship" and
    * outFile + outName + "_entity" will be the output path
    * to store the output temp data
    *
    * after the process about spark
    * scala will use linux console to merge the file to the outMergePath
    * @param mainFiles the file path you need to precess
    * @param outFile output file directory
    * @param outName output file path
    * @param sc spark main context
    * @param index the index of the whole batch
    * @param outMergePath the output merge path
    * @return the schema of the data (entity & relationship)
    */
  def process(mainFiles : String, outFile : String, outName : String , sc : SparkContext, index : Int, outMergePath : String) : Unit = {
    val propertyFlag = "property_tuple_create_by_entityTupleRdd"
    val entityFlag = "entity_tuple_create_by_entityTupleRdd"


    val deletePastTemp = removeFileByShell(outFile)
    if (deletePastTemp == 0) println("done delete past tmp file with code " + deletePastTemp)
    else println("wrong with delete past tmp file with error code " + deletePastTemp)
    var originalRdd : RDD[String] =  sc.textFile(mainFiles)
    //    originalRdd.persist()
    val entityRdd = originalRdd
      .filter(s => regexFunction.named_entity_regex.matcher(s).find() ||
        regexFunction.named_property_regex.matcher(s).find())
    //    println(entityRdd.first())
    //取得n3文件中的关系文件

    val entityTupleRdd : RDD[(String, String, String, String)] = {
      entityRdd.map(s => {
        val me = regexFunction.named_entity_regex.matcher(s)
        val mp = regexFunction.named_property_regex.matcher(s)
        if (me.find()) {
          val id = me.group("id")
          val label = me.group("label")
          val prefix = me.group("prefix")
          (id, label, prefix, entityFlag)}
        else if (mp.find()) {
          val flag = mp.group("flag")
          val value = mp.group("value")
          val name = mp.group("name")
          val names = if (value == flag) flag else name
          ( mp.group("id"),names,value, propertyFlag)
        }
        else null
      }).filter(_ != null)
    }

    //    println("relationship tuple rdd's count is " + relationshipTupleRdd.count())

    val settleUpEntityRdd = entityTupleRdd.map(s => {
      (s._1,s)
    }) //RDD(id, Tuple)
      .groupByKey() //main, gather all the same id entity
      .values //get the same id str


    //    settleUpEntityRdd.checkpoint()
    //    println("entity tuple rdd's count is " + settleUpEntityRdd.count())
    val entitySchema:Seq[String] = settleUpEntityRdd //RDD[Iterable[Tuple]]
      .map(s => s.filter(l => l._4 == propertyFlag)) //RDD[List[PropStr]]
      .map(s => {
      s.map(line => {
        line._2 //get names
      })
    }) // RDD[Iterable[KeyStr]]
      .map(l => {
      l.toSet //RDD[Set[KeyStr]]
    })
      .reduce(_ ++ _) //Set[Key]
      .toSeq
    //    entitySchema.foreach(s => println(s))
    val entityClassRdd = settleUpEntityRdd.map(l => { //RDD[Iterable[Tuple]]
      val en = l.filter(s => s._4 == entityFlag).head //Entity Str

      val label = en._2 //label
      //id
      val id = en._1
      val prefix = en._3
      val prop = l
        .filter(s => s._4 == propertyFlag)//List[PropStr]
        .map(s => {//may cause the duplicated key/value overwrite
        s._2 -> s._3  //name -> value
      }
      )//(key, value)
        .groupBy(i => i._1) //mk the same value to same group then the duplicated one could store in an ARRAY
        .map(f => (f._1, f._2.map(_._2).toArray))
        .toArray
        .toMap
      new Entity(prefix + id, label, prop, entitySchema) // get the new entity
    })

    //    println(entityClassRdd.first().propSeq.length) // 2

    val entityCollectionRdd = entityClassRdd.map(_.toString)
    var entitySchemaDemo : Array[String] = Array("ENTITY_ID:ID")
    entitySchemaDemo ++= entitySchema
    entitySchemaDemo :+= "ENTITY_TYPE:LABEL"

    //    println(cacuArrayEntity.length)
    //    println(entitySchemaDemo.length)

    val cacuArrayEntity = entityClassRdd
      .map(e => e.propSeq.map(_.contains(";"))) //the ';' will only show when the Str is Array
      .reduce( // if this one is duplicated key one then the return will be true
      (a,b) => {
        val ret = for (i <- a.indices) yield {
          a(i) || b(i)
        }
        ret.toArray
      }
    )
    //IndexOutBound
    val finalEntitySchemaDemo =
      for (i <- entitySchemaDemo.indices) yield {
        if (cacuArrayEntity(i))
          entitySchemaDemo(i) + ":String[]"  // this one will use the TRUE/FALSE list to generate the right schema with SchemaType
        else
          entitySchemaDemo(i) // in this case the element is not duplicated one, so the schema will not followed by ":String[]"
      }

    val final_entity_schema = finalEntitySchemaDemo.reduce((a, b) => a + "," + b) // get the final schema, finish the entity schema generate

    println(final_entity_schema)

    val entitySchemaRdd = sc.parallelize(Array(final_entity_schema)) //only way to make rdd
    val be = sc.broadcast(entitySchemaRdd)
    val entityCollectionRddWithHead = be.value ++ entityCollectionRdd // gear up the csv with it's head

    entityCollectionRddWithHead.saveAsTextFile(outFile  + outName +"_entity/") // store the csv file
    println(s"save entity file to ${outFile + outName + "_entity/"}")



    val finalRelationshipSchema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE" // make the relationship schema
    val relationshipRdd = originalRdd
      .filter(s => regexFunction.named_relationship_regex.matcher(s).find())

    val relationshipCollectionRdd : RDD[String] = relationshipRdd.map(s =>
    { // generate the relationship rdd
      val me = regexFunction.named_relationship_regex.matcher(s)
      me.find()
      me.group("prefix1") + me.group("id1") +"," + me.group("type") +"," + me.group("prefix2") + me.group("id2") + "," + me.group("type")
    }
    )

    val finalRelationshipSchemaArray = sc.parallelize(Array(finalRelationshipSchema)) //only way to make rdd
    val br = sc.broadcast(finalRelationshipSchemaArray)
    val relationshipCollectionRddWithHead = br.value ++ relationshipCollectionRdd // build the relationship csv with head
    relationshipCollectionRddWithHead
      .saveAsTextFile(outFile + outName + "_relationship") // store the relationship rdd file

    println(s"save relation file to ${outFile + outName + "_relationship"}")
    //done
//    removeFileByShell(outMergePath + "entity/*")
//
//    removeFileByShell(outMergePath + "relationship/*")

    val mergeEntity = mergeFileByShell(outFile + outName + "_entity", outMergePath + outName + "_entity")

    if (mergeEntity == 0) println("entity merge done with " + mergeEntity)
    else println("wrong with entity merge with error code " + mergeEntity)

    val mergeRelationship = mergeFileByShell(outFile + outName + "_relationship", outMergePath + outName + "_relationship")

    if (mergeRelationship == 0) println("relationship merge done with " + mergeRelationship)
    else println("wrong with relationship merge with error code " + mergeRelationship)
  }

  def mergeFileByShell (filePath : String, outPath : String) : Int = {
    println(s"start to merge file from $filePath/* to $outPath.csv")
    s"hadoop fs -cat $filePath/*" #| s"hadoop fs -put - $outPath.csv"!
  }
}

