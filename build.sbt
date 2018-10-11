

name := "n3Spark"

version := "0.1"

javacOptions++=Seq("-source","1.8","-target","1.8")
//resourceDirectory in Compile := baseDirectory.value / "src/HadoopConf"

scalaVersion := "2.10.5"

scalaVersion in ThisBuild := "2.10.5"

resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
)

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "0.1.0-SNAPSHOT"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "fat-jar-test"
  ).enablePlugins()

assemblyMergeStrategy in assembly := {
  x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.6.3",
  "commons-io" % "commons-io" %  "2.5",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  // https://mvnrepository.com/artifact/com.opencsv/opencsv
  "com.opencsv" % "opencsv" % "4.3.1" % "provided"

  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  //"neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
  //  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
)


//Compile/mainClass := Some("count_the_num")

//unmanagedBase <<= baseDirectory { base => base / "lib" }



//resourceGenerators in Compile += generate( (sourceManaged in Compile).value / "some_directory")


