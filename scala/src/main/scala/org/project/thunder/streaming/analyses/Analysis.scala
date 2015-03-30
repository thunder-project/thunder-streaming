package org.project.thunder.streaming.analyses

import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

import scala.collection.mutable.HashMap

import org.project.thunder.streaming.util.ThunderStreamingContext

object Analysis {
  /*
  Schema:
    <analysis>
      <name>{name}</name>
      <param name={name_1} value={value_1} />
      ...
      <param name={name_n} value={value_n} />
    </analysis>;
   */

  final val OUTPUT = "output"
  final val INPUT = "input"
  final val PREFIX = "prefix"
  final val IDENTIFIER = "identifier"

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
    def extractParameters(nodes: NodeSeq): Try[AnalysisParams] =  {
      val paramNodes = nodes \ "param"
      val ungroupedParams = (paramNodes \\ "@name").map(_.text).zip((paramNodes \\ "@value").map(_.text)).toList
      val groupedParams = ungroupedParams.groupBy{ case (k, v) => k }.map{ case (k, v) => (k, v.map(_._2)) }
      println("groupedParams: %s".format(groupedParams.toString))
      Success(new AnalysisParams(groupedParams))
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
  def instantiateAnalysis[T <: Analysis[_]](clazz: java.lang.Class[T])(tssc: ThunderStreamingContext, params: AnalysisParams): T = {
    val constructors = clazz.getConstructors
    val constructor = constructors(0)
    constructor.newInstance(tssc, params).asInstanceOf[T]
  }
}

class AnalysisParams(val param_map: Map[String, List[String]]) {
  def getParam(key: String): List[String] = param_map.getOrElse(key, List(""))
  def getSingleParam(key: String): String = getParam(key)(0)
}

trait Updatable {
  def handleUpdate(update: (String, String))
}


// Right now there's only ONE namespace for updatable parameters among all the analyses (this can be changed later)

object UpdatableParameters extends Serializable {
  // TODO: This should be made more generic (restricting the value type to String is too limiting)
  val params = new HashMap[String, String]()
  def getUpdatableParam(key: String): Option[String] = params.get(key)
  def setUpdatableParam(key: String, value: String): Unit = params.put(key, value)
}


abstract class Analysis[T <: StreamingData[_, _]](@transient val tssc: ThunderStreamingContext,
                                                  @transient val params: AnalysisParams)
  extends Updatable with Serializable {

  @transient var context = tssc.context

  override def handleUpdate(update: (String, String)): Unit = {
  }

  /**
   * Starts an UpdateListener which connects to the Python-side Analysis object and receives updates (in JSON form)
   * from an external source. These updates must be handled in an Analysis-specific way, and will typically update
   * the updatableVars map.
   */
  def startListening(): Unit = {
    val keySet = Set(DataReceiver.FORWARDER_ADDR, DataReceiver.TAGS)
    val receiverParams = new AnalysisParams(params.param_map.filterKeys(keySet.contains(_)))
    val self = this
    new Thread(new Runnable {
      def run() = {
        // This is NOT thread-safe, but that should be fine.
        // TODO: Would changing updatableParams into a thread-safe data structure affect Spark's serialization?
        val receiver = DataReceiver.getDataReceiver(tssc, self, receiverParams)
        while (!Thread.interrupted()) {
          receiver.receive()
        }
      }
    }).start
  }

  def process(): Unit = {
    val data = load(params.getSingleParam(Analysis.INPUT))
    val out = run(data)
    out.save(params.getSingleParam(Analysis.OUTPUT), params.getSingleParam(Analysis.PREFIX))
    startListening()
  }

  def load(path: String): T

  def run(data: T): T
}

