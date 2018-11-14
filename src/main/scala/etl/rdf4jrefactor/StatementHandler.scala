package etl.rdf4jrefactor

import java.util

import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFHandler, RDFHandlerException}

import scala.collection.mutable


class StatementHandler() extends RDFHandler {
  private val statements = new mutable.HashSet[Statement]
  private val resourceProps = new mutable.HashMap[String, mutable.Map[String, AnyRef]]
  private val resourceLabels = new mutable.HashMap[String, mutable.Set[String]]
  @throws[RDFHandlerException]
  override def startRDF(): Unit = {
  }

  @throws[RDFHandlerException]
  override def endRDF(): Unit = {
  }

  @throws[RDFHandlerException]
  override def handleNamespace(prefix: String, uri: String): Unit = {
  }

  @throws[RDFHandlerException]
  override def handleStatement(st: Statement): Unit = {
    val predicate = st.getPredicate
    val subject = st.getSubject
    //includes blank nodes
    val `object` = st.getObject

    statements.add(st)
  }

  @throws[RDFHandlerException]
  override def handleComment(comment: String): Unit = {
  }

  def getStatement: mutable.HashSet[Statement] = statements
}
