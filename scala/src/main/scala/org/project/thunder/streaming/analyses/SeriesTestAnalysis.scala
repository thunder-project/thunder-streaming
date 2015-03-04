package org.project.thunder.streaming.analyses

import org.project.thunder.streaming.analyses.Analysis.OutputListType
import org.project.thunder.streaming.rdds.StreamingSeries
import org.project.thunder.streaming.regression.StatefulLinearRegression
import org.project.thunder.streaming.util.ThunderStreamingContext

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

class SeriesMeanAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesMean()
  }
}

class SeriesNoopAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data
  }
}

class SeriesStatsAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesStat()
  }
}

class SeriesCountingAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val stats = data.seriesStat()
    val counts = stats.applyValues(arr => Array(arr(0)))
    counts
  }
}

class SeriesCombinedAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val means = data.seriesMean()
    val stats = data.seriesStat()
    val secondMeans = data.seriesMean()
    new StreamingSeries(secondMeans.dstream.union(means.dstream.union(stats.dstream)))
  }
}

class SeriesRegressionAnalysis(tssc: ThunderStreamingContext, params: Map[String, String])
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val slr = new StatefulLinearRegression()
    val fittedStream = slr.runStreaming(data.dstream)
    val weightsStream = fittedStream.map{case (key, model) => (key, model.weights)}
    new StreamingSeries(weightsStream)
  }
}




