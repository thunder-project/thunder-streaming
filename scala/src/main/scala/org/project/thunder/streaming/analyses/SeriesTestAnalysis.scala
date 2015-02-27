package org.project.thunder.streaming.analyses

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.project.thunder.streaming.analyses.Analysis.OutputListType
import org.project.thunder.streaming.rdds.StreamingSeries
import org.project.thunder.streaming.util.ThunderStreamingContext

import scala.util.{Failure, Success, Try}

abstract class SeriesTestAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends Analysis[Int, Array[Double]] {

  def getSeries(): StreamingSeries = {
    val dataPath = params.getOrElse(SeriesTestAnalysis.DATA_PATH_KEY, "series_data")
    tssc.loadStreamingSeries(dataPath, inputFormat = params.getOrElse(SeriesTestAnalysis.FORMAT_KEY, ""))
  }

  def register(outputs: OutputListType): Unit = {
    val data = getSeries()
    val outputFuncs = getOutputFunctions(outputs)
    val analyzedData  = analyze(data)
    analyzedData.outputWithKeys(outputFuncs)
  }

  def analyze(data: StreamingSeries): StreamingSeries

}

object SeriesTestAnalysis {
  final val DATA_PATH_KEY = "data_path"
  final val FORMAT_KEY = "format"
}

class SeriesMeanAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesMean()
  }
}

class SeriesNoopAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data
  }
}

class SeriesStatsAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesStat()
  }
}

class SeriesCountingAnalysis(tssc: ThunderStreamingContext, params: Map[String, String]) extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val stats = data.seriesStat()
    stats.applyValues(arr => arr.)
    stats.applyValues(arr => new Array(arr[0]))
  }
}



