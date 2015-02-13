package org.project.thunder.streaming.outputs

import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Created by Andrew on 2/13/15.
 */
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
        case <name>{ name @ _* }</name> => Success(Class.forName(name(0).text)
          .asSubclass(classOf[AnalysisOutput[StreamingData]]))
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
          case Failure(f) => Failure(f)
        }
      }
      case Failure(f) => Failure(f)
    }
  }

  def instantiateAnalysisOutput[T <: AnalysisOutput[StreamingData]](clazz: java.lang.Class[T])(args:AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    return constructor.newInstance(args:_*).asInstanceOf[T]
  }
}

abstract class AnalysisOutput {
  def handleResult(data: StreamingData): Unit
}

/*
/**
 * An AnalysisOutput can be mixed into an StreamingData object to give that class some specific output capability
 * (i.e. sending results to Lightning, or writing them to disk)
 */
trait AnalysisOutput {
  def handleResults(data: StreamingData, params: Map[String,String]): Unit
}

trait FileSystemOutput extends AnalysisOutput {
  override def handleResults(data: StreamingData, params: Map[String, String]) = {

    writeToPath(data, params("path"))
  }
  def writeToPath(data: StreamingData, path: String): Unit
}

trait LightingOutput extends AnalysisOutput {
  override def handleResults(data: StreamingData, params: Map[String, String]) = {
    sendToLightning(data, params("host"), params("port"))
  }
  def sendToLightning(data: )
}
*/
