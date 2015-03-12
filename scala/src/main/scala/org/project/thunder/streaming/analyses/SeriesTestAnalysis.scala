package org.project.thunder.streaming.analyses

import org.apache.spark.rdd.RDD
import org.project.thunder.streaming.rdds.StreamingSeries

import org.project.thunder.streaming.regression.StatefulLinearRegression
import org.project.thunder.streaming.util.ThunderStreamingContext

import spray.json._
import DefaultJsonProtocol._

abstract class SeriesTestAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
  extends Analysis[StreamingSeries](tssc, params) {

  def load(path: String): StreamingSeries = {
    val format = params.getSingleParam(SeriesTestAnalysis.FORMAT_KEY)
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

class SeriesFiltering1Analysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
    println("SeriesFilteringAnalysis1 setting %s to %s".format("keySet", update._2))
  }

  def analyze(data: StreamingSeries): StreamingSeries = {
    data.dstream.foreachRDD{ rdd: RDD[(Int, Array[Double])] => {
        val keySet = UpdatableParameters.getUpdatableParam("keySet")
        val newRdd = keySet match {
          case Some(k) => {
            val keys: Set[Int] = JsonParser(k).convertTo[List[Int]].toSet[Int]
            if (!keys.isEmpty) {
              rdd.filter { case (k, v) => keys.contains(k)}
            } else {
              rdd
            }
          }
          case _ => rdd
        }
        println("Collected RDD: %s".format(newRdd.take(20).mkString(",")))
      }
    }
    data
  }
}

class SeriesFiltering2Analysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
    println("SeriesFilteringAnalysis2 setting %s to %s".format("keySet", update._2))
  }

  def analyze(data: StreamingSeries): StreamingSeries = {
    val filteredData = data.filterOnKeys{ k => {
      val keySet = UpdatableParameters.getUpdatableParam("keySet")
      println("keySet: %s".format(keySet.toString))
      keySet match {
          case Some(s) => {
            val keys: Set[Int] = JsonParser(s).convertTo[List[Int]].toSet[Int]
            if (k == 0) {
              println("k = %s and keys: %s".format(k.toString, keys.toString))
            }
            (keys.isEmpty) || keys.contains(k)
          }
          case _ => true
        }
      }
    }
    filteredData.print()
    filteredData
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




