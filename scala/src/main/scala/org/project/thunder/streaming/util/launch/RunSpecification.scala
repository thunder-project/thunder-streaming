package org.project.thunder.streaming.util.launch

import org.project.thunder.streaming.analyses.Analysis
import org.project.thunder.streaming.outputs.AnalysisOutput
import org.project.thunder.streaming.rdds.StreamingData

import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq, Node}

/**
 * Created by Andrew on 2/11/15.
 */
class RunSpecification(path: String) {

  type AnalysesList = List[(Try[Analysis[_ <: StreamingData]], Seq[Try[AnalysisOutput[_ <: Array[_]]]])]
  val analyses: AnalysesList = getAnalysesFromXML(scala.xml.XML.loadFile(path))

  def getAnalysesFromXML(nodes: NodeSeq): AnalysesList = {
    nodes.foldLeft(List().asInstanceOf[AnalysesList]) {
      (analysisList, node) => Pair(Analysis.fromXMLNode(node), AnalysisOutput.fromXMLNode(node)) :: analysisList
    }
  }

  def runAnalyses() : Unit = {
    analyses.foreach {
      analysisPair => analysisPair match {
        case (Success(analysis), maybeOutputs) => {
          // We will lazily wait until the analysis is executed before throwing any exceptions
          analysis.run(maybeOutputs)
        }
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
