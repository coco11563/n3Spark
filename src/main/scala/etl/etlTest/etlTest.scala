package etl.etlTest

class etlTest {

}
object etlTest {
  def main(args: Array[String]): Unit = {
    val l = Array(1,2,3,3).toSet
    val l2 = Array(7,3,4,2,3,1,1,1).toSet
//    l.foreach(i => println(i))
    (l ++ l2).foreach( i => println(i))
  }
}
