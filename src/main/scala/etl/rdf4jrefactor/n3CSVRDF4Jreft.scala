package etl.rdf4jrefactor

import java.io.InputStream

import org.apache.commons.io.input.CharSequenceInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
import org.eclipse.rdf4j.rio.ntriples.{NTriplesParser, NTriplesParserFactory}

class n3CSVRDF4Jreft(filePath : String, sc : SparkContext, outputFilePath : String) {
  val n3Str = sc.textFile(filePath).reduce(_ + "\r" + _)
//  val parser : NTriplesParser = new NTriplesParser
  def process(): Unit = {
    val parser = Rio.createParser(RDFFormat.NTRIPLES)
    parser
      .parse(new CharSequenceInputStream(n3Str.toCharArray,"utf-8"), null)
    parser.setRDFHandler(new StatementHandler)
  }
}
object n3CSVRDF4Jreft {
  val HDFSFileSystem: FileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration())
}
