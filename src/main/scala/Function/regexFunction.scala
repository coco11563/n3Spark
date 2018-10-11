package Function

import java.util.regex.{Matcher, Pattern}

object regexFunction {
  def get(m: Matcher, index: Int): String = {
    if (m.find) m.group(index)
    else ""
  }
  //#1 http type_1 #2 id_1 #3 name #4 http type_2 #5 id_2
  final val rela_regex: Pattern = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:[^#][^t][^y][^p][^e])(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)")
  //#1 http type #2 id #3 name #4 value
  final val property_regex: Pattern = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\")(.+)(?:\") (?:\\.)")
  //#1 http type #2 id #3 label
  final val entity_regex: Pattern = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(?:http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:#type)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\\.)")
  //#1 type #2 type(maybe) #2 number
  final val file_regex: Pattern = Pattern.compile("\\/data\\/alldataNew\\/(?<fp1>\\w+)\\/{0,1}(?<fp2>\\w*)\\/(?<name>\\w+).n3")


  //prefix id pprefix name value
  final val named_property_regex : Pattern = Pattern.compile("<(?<prefix>http:\\/\\/[^>]+\\/)(?<id>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> <(?<pprefix>http:\\/\\/[^>]+\\/)(?<name>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> \"(?<value>.+)\" \\.")
  //prefix id lprefix label
  final val named_entity_regex : Pattern = Pattern.compile("<(?<prefix>http:\\/\\/[^>]+\\/)(?<id>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> <(?:http:\\/\\/[^>]+\\/)(?:[^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:#type)> <(?<lprefix>http:\\/\\/[^>]+\\/)(?<label>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> \\.")
  //prefix1 id1 tprefix type prefix2 id2
  final val named_relationship_regex : Pattern = Pattern.compile("<(?<prefix1>http:\\/\\/[^>]+\\/)(?<id1>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> <(?<tprefix>http:\\/\\/[^>]+\\/)(?<type>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?<!#type)> <(?<prefix2>http:\\/\\/[^>]+\\/)(?<id2>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> \\.")

  def isProperty(str : String) : Boolean = {
    property_regex.matcher(str).find()
  }

  def isRelationship(str : String) : Boolean = {
    rela_regex.matcher(str).find()
  }

  def isEntity(str : String) : Boolean = {
    entity_regex.matcher(str).find()
  }

  def getValue(p : Pattern, str:String, index: Int) : String = {
    p.matcher(str).group(index)
  }

  def get(m : Matcher, name : String) : String = {
    if (m.find) m.group(name)
    else ""
  }

  def main(args: Array[String]): Unit = {
    val m = regexFunction.named_property_regex.matcher("<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17> <http://gcm.wdcm.org/ontology/gcmAnnotation/v1/otherName> \"ALPDH\" .")
    println(get(m, "pprefix"))
    println(get(m, "name"))
  }
}
