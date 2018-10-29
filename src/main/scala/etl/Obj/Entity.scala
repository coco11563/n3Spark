package etl.Obj

import java.io.StringWriter

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

class Entity( id : String, label : String, prop : Map[String, Array[String]], schema : Seq[String]) extends Serializable
{
  def propSeq : Array[String] = {
    var ret : ListBuffer[String] = new ListBuffer[String]()
    ret +:= id
    val l =  for (name <- schema) yield {
      val value : Array[String] = {
        if (prop.contains(name))
          prop(name)
        else
          Array("")
      }
      value
        .map(f => if (f == "") "\"\"" else f)
        .map(_.replaceAll(";", " "))
        .reduce((a,b) => a + ";" + b)
    }
    ret ++= l
    ret += label
    ret.map(s => {
      if (s.contains(","))
        "\"" + s + "\""
      else
        s
    }).toArray
  }

  override def toString: String = {
    this.propSeq.reduce((a,b) => a + "," + b)
  }
  def toRow : Row = {
    Row(propSeq)
  }

}
object Entity{
  def main(args: Array[String]): Unit = {
    val m : Map[String, Array[String]] = Map("sc1" ->Array("test1"), "sc2" -> Array(",2","1"))
    val e = new Entity("la,bel1", "i,d2", m, Array("sc1","sc2"))
    val stringWriter = new StringWriter()
  }
}