package Function.test

import Function.regexFunction

class regexTest {

}
object regexTest {
  def main(args: Array[String]): Unit = {
    val m1 = regexFunction.file_regex.matcher("/data/alldataNew/1/3.n3")
    val m2 = regexFunction.file_regex.matcher("/data/alldataNew/1/2/3.n3")

    val m3 = regexFunction.named_property_regex.matcher("<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.2.4.1> <http://gcm.wdcm.org/ontology/gcmAnnotation/v1/keggGene> \"ncr:NCU06482\" .")


    if (m1.find()) {
      println(m1.group())
      println(m1.group(1))
      println(m1.group(2).length)
      println(m1.group(3))
    }
    if (m2.find()) {
      println(m2.group())
      println(m2.group("fp2"))
      println(m2.group("fp1"))
      println(m2.group("name"))
    }
    println(regexFunction.get(m3, "id"))
    if (m3.find()) {
      println(m3.group())
      println(m3.group("id"))
      println(m3.group("type"))
      println(m3.group("prefix"))
    }


  }
}
