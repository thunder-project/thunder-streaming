package org.project.thunder.streaming.analyses

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.project.thunder.streaming.analyses.Analysis.OutputListType
import org.project.thunder.streaming.outputs.AnalysisOutput
import org.project.thunder.streaming.util.ThunderStreamingContext

import scala.util.{Failure, Success, Try}

/**
 * Created by Andrew on 2/13/15.
 */
class SimpleExampleAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends Analysis[Int, Array[Double]] {

  def register(outputs: OutputListType): Unit = {

    val dataPath = params.getOrElse(SimpleExampleAnalysis.DATA_PATH_KEY, "series_data")

    val data = tssc.loadStreamingSeries(dataPath, inputFormat = "text")

    val means = data.seriesMean()

    val outputFunctions = super.getOutputFunctions(outputs)

    means.outputWithKeys(outputFunctions)
  }
}

object SimpleExampleAnalysis {
  final val DATA_PATH_KEY = "data_path"
}

