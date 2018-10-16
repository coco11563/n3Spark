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
  final val file_regex: Pattern = Pattern.compile("\\/data\\/alldataNew\\/(?<fp1>\\w+)\\/{0,1}(?<fp2>\\w*)\\/(?<name>[\\w.]+).n3")
  //fname
  final val csv_entity_part_regex : Pattern = Pattern.compile("\\/data\\/out\\/entity\\/(?<fname>(?:\\w+)\\.csv)\\/part-[\\d]{5}")
  //fname
  final val csv_relationship_part_regex : Pattern = Pattern.compile("\\/data\\/out\\/relationship\\/(?<fname>(?:\\w+)\\.csv)\\/part-[\\d]{5}")
  //prefix id pprefix name value
  final val named_property_regex : Pattern = Pattern.compile("<(?<prefix>http:\\/\\/[^>]+\\/)(?<id>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> <(?<pprefix>http:\\/\\/[^>]+\\/)(?<name>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> \"(?<value>.+)\" \\.")
  //prefix id lprefix label
  final val named_entity_regex : Pattern = Pattern.compile("(<(?<prefix>http:\\/\\/[^>]+\\/)(?<id>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)>|(?<nonprefixid>[:_\\w]+)) <(?:http:\\/\\/[^>]+\\/)(?:[^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:#type)> <(?<lprefix>http:\\/\\/[^>]+\\/)(?<label>[^\\/][-A-Za-z0-9._#$%^&*!@~]+)> \\.")
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
    val m = regexFunction.named_entity_regex.matcher("_:BX2D6caf4877X3A1588628adf8X3AX2D4827 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Axiom> .")
    var id = if (m.find()) {
      if (m.group("prefix") == null) {
        m.group("nonprefixid")
      } else {
        m.group("prefix") + m.group("id")
      }
    }
    println(getEntityId("_:BX2D6caf4877X3A1588628adf8X3AX2D4827 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Axiom> ."))
    println(getEntityId("<http://www.w3.org/2002/07/owl#annotatedSource> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Axiom> ."))
  }

  def getEntityId(str : String) : String = {
    val m = regexFunction.named_entity_regex.matcher(str)
    var id = if (m.find()) {
      if (m.group("prefix") == null) {
        m.group("nonprefixid")
      } else {
        m.group("prefix") + m.group("id")
      }
    } else {
      ""
    }
    id
  }
}
