package etl.etlTest
import breeze.linalg._
class etlTest {

}
object etlTest {
  def main(args: Array[String]): Unit = {
    val l = Array(1,2,3,3).toSet
    val l2 = Array(7,3,4,2,3,1,1,1).toSet
//    l.foreach(i => println(i))
    (l ++ l2).foreach( i => println(i))
    val vec1 = DenseVector[Boolean](Array(true, true, false, false))
    val vec2 = DenseVector[Boolean](Array(false, false, true, true))
    val ary1 = Array(true, true, false, false)
    val ary2 = Array(false, false, true, true)

    val t1 = System.nanoTime
    for(i <- 0 to 10000000) {
      for (j <- ary1.indices) yield {
        ary1(j) || ary2(j)
      }
    }
    val duration_1 = (System.nanoTime - t1) / 1e9d
    val t2 = System.nanoTime
    for(i <- 0 to 10000000) {
      vec2 | vec1
    }
    val duration_2 = (System.nanoTime - t2) / 1e9d
    (vec2 | vec1).data.foreach(println(_))
    println("run time in t1 = " + duration_1)
    println("run time in t2 = " + duration_2)
  }
}
