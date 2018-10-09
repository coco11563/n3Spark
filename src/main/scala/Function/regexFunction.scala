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
  //#1 type #2 number
  final val file_regex: Pattern = Pattern.compile("(?:\\/data\\/alldataNew\\/)(\\w+)(?:\\/)(\\w+)(?:\\.n3)")

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
}
