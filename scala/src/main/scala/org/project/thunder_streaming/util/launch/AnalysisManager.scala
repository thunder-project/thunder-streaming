package org.project.thunder_streaming.util.launch

import org.project.thunder_streaming.analyses.Analysis
import org.project.thunder_streaming.util.ThunderStreamingContext

import scala.util.{Failure, Success}

class AnalysisManager(tssc: ThunderStreamingContext, path: String) {

  val analyses: List[Analysis[_]] = load(tssc, path)

  def load(tssc: ThunderStreamingContext, path: String): List[Analysis[_]] = {
    val root = scala.xml.XML.loadFile(path)

    // For each <analysis> child of the root, generate an Analysis object (and its associated AnalysisOutputs) to generate
    // the list of analyses
    (root \\ "analysis").foldLeft(List().asInstanceOf[List[Analysis[_]]]) {
      (analysisList, node) => {
        val maybeAnalysis = Analysis.instantiateFromConf(tssc, node)
        maybeAnalysis match {
          case Success(analysis) => analysis :: analysisList
          case Failure(f) => {
            f.printStackTrace()
            analysisList
          }
        }
      }
    }
  }
}
