package org.project.thunder.streaming.util.launch

import org.project.thunder.streaming.rdds.StreamingData



import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect._
import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq, Node}

/**
 * Created by Andrew on 2/11/15.
 */
class RunSpecification(path: String) {

  val analyses = getAnalysesFromXML(scala.xml.XML.loadFile(path))

  object Analysis {
    /*
    Schema:
      <analysis>
        <name>{name}</name>
        <param name={name_1} value={value_1} />
        ...
        <param name={name_n} value={value_n} />
        <output>{(see AnalysisOutput Schema )}</output>
      </analysis>;
     */

    class BadAnalysisConfigException(msg: String) extends RuntimeException(msg)

    def fromXMLNode(nodes: NodeSeq): Try[Analysis[StreamingData]] = {
      // Try to find a class with the given type name
      def extractAndFindClass(nodes: NodeSeq): Try[Class[_ <: Analysis[StreamingData]]] = {
        nodes \ "name" match {
          case <name>{ name @ _* }</name> => Success(Class.forName(name(0).text).asSubclass(Analysis[StreamingData]))
          case _ => Failure(new BadAnalysisConfigException("Name not correctly specified in XML configuration file."))
        }
      }
      // Extract all parameters necessary to instantiate an instance of the above output type
      def extractParameters(nodes: NodeSeq): Try[Map[String,String]] =  {
        val paramNodes = nodes \ "param"
        val paramList = ((paramNodes \\ "@name").map(_.text)).zip((paramNodes \\ "@value").map(_.text)).toList
        Success(Map(paramList: _*))
      }
      // Attempt to invoke the (maybe) AnalysisOutput class' constructor with paramMap as an argument
      // Not using for..yield because I want Failures to propagate out of this method
      extractAndFindClass(nodes) match {
        case Success(clazz) => {
          extractParameters(nodes) match {
            case Success(parameters) => Try(instantiateAnalysis(clazz)(parameters))
            case _ => _
          }
        }
        case _ => _
      }
    }

    // Code modified from http://stackoverflow.com/questions/1641104/instantiate-object-with-reflection-using-constructor-arguments
    def instantiateAnalysis[T <: Analysis[StreamingData]](clazz: java.lang.Class[T])(args:AnyRef*): T = {
      val constructor = clazz.getConstructors()(0)
      return constructor.newInstance(args:_*).asInstanceOf[T]
    }
  }

  trait Analysis[T <: StreamingData] {
    def run(output: AnalysisOutput[T]): Unit;
  }

  object AnalysisOutput {
    /*
    An AnalysisOutput is specified with the following schema. Each <output> tag must only have one <type> tag, and
    may have zero or more <param> tags.

    Schema:
    <output>
      <name>{type}</name>
      <param name={name_1} value={value_1} />
      ...
      <param name={name_n} value={value_n} />
    </output>;
    */

    class BadOutputConfigException(msg: String) extends RuntimeException(msg)

    def fromXMLNode(nodes: NodeSeq): Try[AnalysisOutput[StreamingData]] = {
      // Try to find a class with the given type name
      def extractAndFindClass(nodes: NodeSeq): Try[Class[_ <: AnalysisOutput[StreamingData]]] = {
        nodes \ "name" match {
          case <name>{ name @ _* }</name> => Success(Class.forName(name(0).text).asSubclass(AnalysisOutput[StreamingData]))
          case _ => Failure(new BadOutputConfigException("Name not correctly specified in XML configuration file."))
        }
      }
      // Extract all parameters necessary to instantiate an instance of the above output type
      def extractParameters(nodes: NodeSeq): Try[Map[String,String]] =  {
        val paramNodes = nodes \ "param"
        val paramList = ((paramNodes \\ "@name").map(_.text)).zip((paramNodes \\ "@value").map(_.text)).toList
        Success(Map(paramList: _*))
      }
      // Attempt to invoke the (maybe) AnalysisOutput class' constructor with paramMap as an argument
      // Not using for..yield because I want Failures to propagate out of this method
      extractAndFindClass(nodes) match {
        case Success(clazz) => {
          extractParameters(nodes) match {
            case Success(parameters) => Try(instantiateAnalysisOutput(clazz)(parameters))
            case _ => _
          }
        }
        case _ => _
      }
    }

    def instantiateAnalysisOutput[T <: AnalysisOutput[StreamingData]](clazz: java.lang.Class[T])(args:AnyRef*): T = {
      val constructor = clazz.getConstructors()(0)
      return constructor.newInstance(args:_*).asInstanceOf[T]
    }
  }

  abstract class AnalysisOutput[T <: StreamingData](val params: Map[String, String] = Map()) {
    def handleResults(results: T): Unit
  }

  def getAnalysesFromXML(nodes: NodeSeq): List[(Try[Analysis[_]], Try[AnalysisOutput[_]])] = {
    nodes.foldLeft(List[Pair[Try[Analysis[_]], Try[AnalysisOutput[_]]]]()) {
      (analysisList, node) => Pair(Analysis.fromXMLNode(node), AnalysisOutput.fromXMLNode(node)) :: analysisList
    }
  }

  def runAnalyses() : Unit = {
    analyses.foreach {
      analysisPair => analysisPair match {
        case (Success(analysis), Success(output)) => analysis.run(output)
        // For now, just print the thrown exceptions if there were errors
        // Also, is there a better way of dealing with Pairs?
        case (Failure(f1), Failure(f2)) => {
          println(f1)
          println(f2)
        }
        case (Failure(f1), _) => println(f1)
        case (_, Failure(f2)) => println(f2)
      }
    }
  }
}
