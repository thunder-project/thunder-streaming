package org.project.thunder.streaming.analyses

import org.project.thunder.streaming.outputs.AnalysisOutput
import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Created by Andrew on 2/13/15.
 */

object Analysis {
  /*
  Schema:
    <analysis>
      <name>{name}</name>
      <param name={name_1} value={value_1} />
      ...
      <param name={name_n} value={value_n} />
      <output>{(see AnalysisOutput Schema )}</output>
      <output>...</output>
    </analysis>;
   */

  class BadAnalysisConfigException(msg: String) extends RuntimeException(msg)

  def fromXMLNode(nodes: NodeSeq): Try[Analysis[StreamingData]] = {
    // Try to find a class with the given type name
    def extractAndFindClass(nodes: NodeSeq): Try[Class[_ <: Analysis[StreamingData]]] = {
      nodes \ "name" match {
        case <name>{ name @ _* }</name> => Success(Class.forName(name(0).text)
          .asSubclass(classOf[Analysis[StreamingData]]))
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
          case Failure(f) => Failure(f)
        }
      }
      case Failure(f) => Failure(f)
    }
  }

  // Code modified from http://stackoverflow.com/questions/1641104/instantiate-object-with-reflection-using-constructor-arguments
  def instantiateAnalysis[T <: Analysis[StreamingData]](clazz: java.lang.Class[T])(args:AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    return constructor.newInstance(args:_*).asInstanceOf[T]
  }
}

trait Analysis[T <: StreamingData] {
  // TODO: There doesn't seem to be much else we can do as far as better type-checking is concerned
  def run(outputs: List[Try[AnalysisOutput[_ <: StreamingData]]]): Unit
}

