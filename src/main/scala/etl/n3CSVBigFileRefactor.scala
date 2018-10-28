package etl

import Function.{HDFSHelper, regexFunction}
import etl.Obj.Entity
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.sys.process._

class n3CSVBigFileRefactor {

}
object n3CSVBigFileRefactor {
  val HDFSFileSystem: FileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration())
  def main(args: Array[String]): Unit = {
    val mainFilepath = args(0)
    val outName = args(1)
    val countPer = Integer.parseInt(args(2))
    val outFile = "/data/out/temp/"
    val tempMergePath = "/data/out/merge/"
    val mergePath = "/data/out/csv/mergeCSV/"

    val sparkConf = new SparkConf()
      .setAppName("ProcessOn" + outName)
      .set("spark.driver.maxResultSize", "4g")


    val pathList : List[String] = HDFSHelper
      .listChildren(HDFSFileSystem, mainFilepath, new ListBuffer[String])
      .toList

//    pathList.foreach(println(_))

    val sc = new SparkContext(sparkConf)
    var v = new ListBuffer[(Seq[String], Seq[String])]
    var list : ListBuffer[String] = new ListBuffer[String]
    println(pathList.head)
    println("=================================================")
    for (i <- pathList.indices) {
      println(s"===================$i==============================")
      println(s"=================$countPer=========================")
      if (i % countPer == 0 && i != 0) {
        list += pathList(i)
        v += process(list.toList, outFile, outName, sc, i, tempMergePath)
        list = new ListBuffer[String]
      } else if (i == pathList.size - 1) {
        list += pathList(i)
        v += process(list.toList, outFile, outName, sc, i, tempMergePath)
      }else {list += pathList(i)}

    }
    val head_entity = v.toList.head._1
    val head_relationship = v.toList.head._2

    var entityMerge = HDFSHelper.listChildren(HDFSFileSystem, tempMergePath + "/entity", new ListBuffer[String])
    var relationshipMerge = HDFSHelper.listChildren(HDFSFileSystem, tempMergePath + "/relationship", new ListBuffer[String])

    sc.parallelize(head_relationship).saveAsTextFile(tempMergePath + "/relationship/head")
    println(s"save head relationship to $tempMergePath/realtaionship/head")
    sc.parallelize(head_entity).saveAsTextFile(tempMergePath + "/entity/head")
    println(s"save head entity to $tempMergePath/entity/head")
    //delete the stuff in the final merge path
    removeFileByShell(mergePath + outName + "/")
    //adding the head to the first of the list
    entityMerge +:=  tempMergePath + "/entity/head"
    relationshipMerge +:= tempMergePath + "/relationship/head"

    relationshipMerge.foreach(mergeFileByShell(_ , mergePath + outName + "/" + outName + "_relationship" + ".csv"))
    entityMerge.foreach(mergeFileByShell(_, mergePath + outName + "/" + outName + "_entity" + ".csv"))
  }

  /**
    *
    * @param filePath the path you need to remove(-r)
    * @return the result 0 means be all set
    */
  def removeFileByShell (filePath : String) : Int = {
    println(s"removing the file from $filePath")
    s"hadoop fs -rm -r $filePath"!
  }

  /**
    *
    * @param filePath input file directory
    * @param outPath output file directory
    * @return the result 0 means be all set
    */
  def mergeFileByShell (filePath : String, outPath : String) : Int = {
    println(s"start to merge file from $filePath to $outPath")
    if(!HDFSHelper.exists(HDFSFileSystem,outPath)) {
      println(s"start to merge file from $filePath to $outPath : $outPath not existed, so create it")
      s"hdfs dfs -cat $filePath" #| s"hdfs dfs -put - $outPath" !
    }
    else {
      println(s"start to merge file from $filePath to $outPath : $outPath existed, appending")
      s"hdfs dfs -cat $filePath" #| s"hdfs dfs -appendToFile - $outPath"!
    }

  }

  /**
    *
    * @param filePath input file directory
    * @param outPath output file name(with index)
    * @param index this file's index
    * @return the result 0 means be all set
    */
  def mergeFileByShell (filePath : String, outPath : String, index : Int) : Int = {
    println(s"start to merge file from $filePath/* to ${outPath}_$index.csv")
    s"hadoop fs -cat $filePath/*" #| s"hadoop fs -put - ${outPath}_$index.csv"!
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
  def process(mainFiles : List[String], outFile : String, outName : String , sc : SparkContext, index : Int, outMergePath : String) : (Seq[String], Seq[String]) = {
    val deletePastTemp = removeFileByShell(outFile)
    if (deletePastTemp == 0) println("done delete past tmp file with code " + deletePastTemp)
    else println("wrong with delete past tmp file with error code " + deletePastTemp)
    var originalRdd : RDD[String] = sc.emptyRDD[String]
    for (s <- mainFiles) {
      originalRdd = originalRdd.union(sc.textFile(s))
    }
    println(originalRdd.first())
    val entityRdd = originalRdd
      .filter(s => regexFunction.named_entity_regex.matcher(s).find() ||
        regexFunction.named_property_regex.matcher(s).find())
    //    println(entityRdd.first())
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
      .map(e => e.propSeq.map(_.contains(";"))) //the ';' will only show when the Str is Array
      .reduce( // if this one is duplicated key one then the return will be true
      (a,b) => {
        val ret = for (i <- a.indices) yield {
          a(i) || b(i)
        }
        ret.toArray
      }
       //using breeze lib to replace the yield operation
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

    val final_entity_schema = finalEntitySchemaDemo.reduce((a, b) => a + "," + b) // get the final schema, finish the entity schema generate

//    val entitySchemaRdd = sc.parallelize(Array(final_entity_schema)) //only way to make rdd
//    val entityCollectionRddWithHead = entitySchemaRdd ++ entityCollectionRdd // gear up the csv with it's head

    entityCollectionRdd.saveAsTextFile(outFile  + outName +"_entity/") // store the csv file
    println(s"save entity file to ${outFile + outName + "_entity/"}")
    val finalRelationshipSchema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE" // make the relationship schema

    val relationshipCollectionRdd = relationshipRdd.map(s => { // generate the relationship rdd
      regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id1") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"id2") + "," +
        regexFunction.get(regexFunction.named_relationship_regex.matcher(s),"type")
    })

//    val relationshipCollectionArray = sc.parallelize(Array(finalRelationshipSchema)) //only way to make rdd
//    val relationshipCollectionArrayRdd = relationshipCollectionArray ++ relationshipCollectionRdd // build the relationship csv with head
    relationshipCollectionRdd
      .saveAsTextFile(outFile + outName + "_relationship") // store the relationship rdd file
    println(s"save relation file to ${outFile + outName + "_relationship"}")
//done

    val mergeEntity = mergeFileByShell(outFile + outName + "_entity", outMergePath + "entity/" + outName, index)

    if (mergeEntity == 0) println("entity merge done with " + mergeEntity)
    else println("wrong with entity merge with error code " + mergeEntity)

    val mergeRelationship = mergeFileByShell(outFile + outName + "_relationship", outMergePath + "relationship/" + outName, index)

    if (mergeRelationship == 0) println("relationship merge done with " + mergeRelationship)
    else println("wrong with relationship merge with error code " + mergeRelationship)


    (finalEntitySchemaDemo, finalRelationshipSchema.split(","))
  }
}

