package org.project.thunder.streaming.util.launch

import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq, Node}

/**
 * Created by Andrew on 2/11/15.
 */
object AnalysesSpec {

  object Analysis {
    /*
    Schema:
      <analysis>
        <name>{name}</name>
        <output>{ AnalysisOutput Schema }</output>
      </analysis>;
     */

    class BadAnalysisConfigException(msg: String) extends RuntimeException(msg)

    def fromXMLNode(nodes: NodeSeq):  Try[Analysis] = {
      nodes \ "name" match {
        case <name>{ name @ _* }</name> => Try(Class.forName(name(0).text).newInstance().asInstanceOf[Analysis])
        case _ => Failure(new BadAnalysisConfigException("Name not correctly specified in XML configuration file."))
      }
    }
  }

  trait Analysis[T <: StreamingData] {
    def getOutputClass(): Class[T] = classOf[T]
    def run(output: AnalysisOutput[T]): Unit;
  }

  object AnalysisOutput {
    /*
    An AnalysisOutput is specified with the following schema. Each <output> tag must only have one <type> tag, and
    may have zero or more <param> tags.

    Schema:
    <output>
      <type>{type}</type>
      <param name={name} value={value} />
    </output>;
    */

    class BadOutputConfigException(msg: String) extends RuntimeException(msg)

    def fromXMLNode(nodes: NodeSeq): Try[AnalysisOutput] = {
      // Try to find a class with the given type name
      val maybeTypeClass = (nodes \ "type") match {
        case <type>{ typeName @ _* }</type> => Try(Class.forName(typeName(0).text))
        case _ => Failure(new BadOutputConfigException("Name not correctly specified in XML configuration file."))
      }
      // Extract all parameters necessary to instantiate an instance of the above output type
      val paramNodes = nodes \ "param"
      val paramList = ((paramNodes \\ "@name").map(_.text)).zip((paramNodes \\ "@value").map(_.text)).toList
      val paramMap = Map(paramList: _*)
      // Attempt to invoke the (maybe) AnalysisOutput class' constructor with paramMap as an argument
      maybeTypeClass.flatMap(clazz => Try(clazz.getConstructors()(0).newInstance(paramMap).asInstanceOf[AnalysisOutput]))
    }
  }

  abstract class AnalysisOutput[T <: StreamingData](val params: Map[String, String] = Map()) {
    def getInputClass: Class[T] = classOf[T]
    def handleResults(results: T): Unit
  }

  def getAnalysesFromXML(nodes: NodeSeq): Try[List[(Analysis, AnalysisOutput)]] =  {
    nodes.foldLeft(List()) {
      (analysisList, node) => (Analysis.fromXMLNode(node), AnalysisOutput.fromXMLNode(node)) match {
        case pair @ (Success(analysis), Success(output)) => pair : analysisList
        case (f @ Failure(_), _) => f
        case (_, f @ Failure(_)) => f
      }
    }
  }
}
