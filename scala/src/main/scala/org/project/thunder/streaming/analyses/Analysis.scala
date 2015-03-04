package org.project.thunder.streaming.analyses

import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

import org.project.thunder.streaming.util.ThunderStreamingContext

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

  final val OUTPUT = "output"
  final val INPUT = "input"
  final val PREFIX = "prefix"

  class BadAnalysisConfigException(msg: String) extends RuntimeException(msg)

  def instantiateFromConf(tssc: ThunderStreamingContext, nodes: NodeSeq): Try[Analysis[_]] = {
    // Try to find a class with the given type name
    def extractAndFindClass(nodes: NodeSeq): Try[Class[_ <: Analysis[_]]] = {
      val nameNodes = nodes \ "name"
      if (nameNodes.length != 1) {
        Failure(new BadAnalysisConfigException("The Analysis name was not correctly specified in a single <name> element"))
      } else {
        nameNodes(0) match {
          case <name>{ name @ _* }</name> => Success(Class.forName(name(0).text)
            .asSubclass(classOf[Analysis[_]]))
          case _ => Failure(new BadAnalysisConfigException("The Analysis name was not correctly specified"))
        }
      }
    }
    // Extract all parameters necessary to instantiate an instance of the above output type
    def extractParameters(nodes: NodeSeq): Try[Map[String,String]] =  {
      val paramNodes = nodes \ "param"
      val paramList = (paramNodes \\ "@name").map(_.text).zip((paramNodes \\ "@value").map(_.text)).toList
      Success(Map(paramList: _*))
    }
    // Attempt to invoke the (maybe) AnalysisOutput class' constructor with paramMap as an argument
    // Not using for..yield because I want Failures to propagate out of this method
    extractAndFindClass(nodes) match {
      case Success(clazz) => extractParameters(nodes) match {
        case Success(parameters) => Try(instantiateAnalysis(clazz)(tssc, parameters))
        case Failure(f) => Failure(f)
      }
      case Failure(f) => Failure(f)
    }
  }

  // Code modified from http://stackoverflow.com/questions/1641104/instantiate-object-with-reflection-using-constructor-arguments
  def instantiateAnalysis[T <: Analysis[_]](clazz: java.lang.Class[T])(args:AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(args:_*).asInstanceOf[T]
  }
}

abstract class Analysis[T <: StreamingData[_, _]](tssc: ThunderStreamingContext, params: Map[String, String]) {

  def getParam(key: String): String = params.getOrElse(key, "")

  def process(): Unit = {
    val data = load(getParam(Analysis.INPUT))
    val out = run(data)
    out.save(getParam(Analysis.OUTPUT), getParam(Analysis.PREFIX))
  }

  def load(path: String): T

  def run(data: T): T
}

