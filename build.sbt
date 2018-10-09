

name := "n3Spark"

version := "0.1"

javacOptions++=Seq("-source","1.8","-target","1.8")
//resourceDirectory in Compile := baseDirectory.value / "src/HadoopConf"

scalaVersion := "2.10.5"

//externalResolvers:= Resolver.DefaultMavenRepository(resolvers.value, false)

libraryDependencies ++= Seq(
//  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "commons-io" % "commons-io" %  "2.5"
//  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
)
mainClass in (Compile, run) := Some("count_the_num")
mainClass in (Compile, packageBin) := Some("count_the_num")
//Compile/mainClass := Some("count_the_num")

//unmanagedBase <<= baseDirectory { base => base / "lib" }



//resourceGenerators in Compile += generate( (sourceManaged in Compile).value / "some_directory")


