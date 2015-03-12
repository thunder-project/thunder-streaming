package org.project.thunder.streaming.analyses

import org.project.thunder.streaming.rdds.StreamingSeries
import org.project.thunder.streaming.regression.StatefulLinearRegression
import org.project.thunder.streaming.util.ThunderStreamingContext

abstract class SeriesTestAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends Analysis[StreamingSeries](tssc, params) {

  def load(path: String): StreamingSeries = {
    val format = params.getSingleParam(SeriesTestAnalysis.FORMAT_KEY)
    println("In load, decoding format: %s".format(format))
    tssc.loadStreamingSeries(path, inputFormat = format)
  }

  override def run(data: StreamingSeries): StreamingSeries = {
    analyze(data)
  }

  def analyze(data: StreamingSeries): StreamingSeries

}

object SeriesTestAnalysis {
  final val DATA_PATH_KEY = "data_path"
  final val FORMAT_KEY = "format"
}

class SeriesMeanAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesMean()
  }
}

class SeriesNoopAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data
  }
}

class SeriesStatsAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    data.seriesStats()
  }
}

class SeriesCountingAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val stats = data.seriesStats()
    val counts = stats.applyValues(arr => Array(arr(0)))
    counts
  }
}

class SeriesCombinedAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val means = data.seriesMean()
    val stats = data.seriesStats()
    val secondMeans = data.seriesMean()
    new StreamingSeries(secondMeans.dstream.union(means.dstream.union(stats.dstream)))
  }
}

class SeriesRegressionAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val slr = new StatefulLinearRegression()
    val fittedStream = slr.runStreaming(data.dstream)
    val weightsStream = fittedStream.map{case (key, model) => (key, model.weights)}
    new StreamingSeries(weightsStream)
  }
}




