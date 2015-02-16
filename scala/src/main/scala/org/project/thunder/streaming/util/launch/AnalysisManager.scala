package org.project.thunder.streaming.util.launch

import org.project.thunder.streaming.analyses.Analysis
import org.project.thunder.streaming.outputs.AnalysisOutput
import org.project.thunder.streaming.util.ThunderStreamingContext

import scala.util.{Failure, Success, Try}
import scala.xml.{NodeSeq}

/**
 * Created by Andrew on 2/11/15.
 */
class AnalysisManager(tssc: ThunderStreamingContext, path: String) {

  type AnalysesList = List[(Try[Analysis[_, _]], List[Try[AnalysisOutput[_ <: List[_]]]])]

  val analyses: AnalysesList = load(tssc, path)

  def load(tssc: ThunderStreamingContext, path: String): AnalysesList = {
    val root = scala.xml.XML.loadFile(path)

    // For each <analysis> child of the root, generate an Analysis object (and its associated AnalysisOutputs) to generate
    // the list of analyses
    (root \\ "analysis").foldLeft(List().asInstanceOf[AnalysesList]) {
      (analysisList, node) => {
        Pair(Analysis.instantiateFromConf(tssc, node), AnalysisOutput.instantiateFromConf(node)) :: analysisList
      }
    }
  }

  def register() : Unit = {
    analyses.foreach {
      analysisPair => analysisPair match {
        case (Success(analysis), maybeOutputs) => {
          // We will lazily wait until the analysis is executed before throwing any exceptions
          analysis.register(maybeOutputs)
        }
        // For now, just print the thrown exceptions if there were errors
        // Also, is there a better way of dealing with Pairs?
        case Pair(Failure(f1), _) => println(f1)
      }
    }
  }
}
