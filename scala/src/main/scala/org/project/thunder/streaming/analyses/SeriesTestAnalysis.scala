package org.project.thunder.streaming.analyses

import org.apache.spark.rdd.RDD
import org.project.thunder.streaming.rdds.StreamingSeries

import org.project.thunder.streaming.regression.StatefulLinearRegression
import org.project.thunder.streaming.util.ThunderStreamingContext
import org.apache.spark.SparkContext._

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
    val mean = data.seriesMean()
    mean
  }
}

class SeriesBatchMeanAnalysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {
  def analyze(data: StreamingSeries): StreamingSeries = {
    val batchMean = data.dstream.map{ case (k, v) => (k, Array(v.reduce(_ + _) / v.size)) }
    new StreamingSeries(batchMean)
  }
}

class SeriesFiltering1Analysis(tssc: ThunderStreamingContext, params: AnalysisParams)
    extends SeriesTestAnalysis(tssc, params) {

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
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

  val partitionSize = params.getSingleParam("partition_size").toInt
  val dims = params.getSingleParam("dims").parseJson.convertTo[List[Int]]

  def getKeysFromJson(keySet: Option[String], dims: List[Int]): List[Set[Int]]= {
    val parsedKeys = keySet match {
        case Some(s) => {
          JsonParser(s).convertTo[List[List[List[Double]]]]
        }
        case _ => List()
    }
    val keys = parsedKeys.map(_.map(key => {
        key.zipWithIndex.foldLeft(0){ case (sum, (dim, idx)) => (sum + (dims(idx) * dim)).toInt }
    }).toSet[Int])
    println("keys: %s, dims: %s".format(keys.toString, dims.toString))
    keys
  }

  override def handleUpdate(update: (String, String)): Unit = {
    UpdatableParameters.setUpdatableParam("keySet", update._2)
  }

  def analyze(data: StreamingSeries): StreamingSeries = {
    val filteredData = data.dstream.transform { rdd =>

      val keySet = UpdatableParameters.getUpdatableParam("keySet")

      val keys = getKeysFromJson(keySet, dims)

      val withIndices = keys.zipWithIndex
      val setSizes = withIndices.foldLeft(Map[Int, Int]()) {
        (curMap, s) => curMap + (s._2 -> s._1.size)
      }

      // Reindex the (k,v) pairs with their set inclusion values as K
      println("Before first RDD operation")
      val mappedKeys = rdd.flatMap { case (k, v) =>
        val setMatches = withIndices.map { case (set, i) => if (set.contains(k)) (i, v) else (-1, v)}
        setMatches.filter { case (k, v) => k != -1}
      }
      println("After first RDD operation")

      // For each set, compute the mean time series (pointwise addition divided by set size)
      val sumSeries = mappedKeys.reduceByKey((arr1, arr2) => arr1.zip(arr2).map { case (v1, v2) => v1 + v2})
      val meanSeries = sumSeries.map { case (idx, sumArr) => (idx, sumArr.map(x => x / setSizes(idx)))}

      // Do some temporal averaging on the (spatial) mean time series
      val avgSeries = meanSeries.map{ case (idx, meanArray) => (idx, meanArray.sliding(partitionSize).map(x => x.reduce(_+_) / x.size).toArray[Double]) }
      println("avgSeries.first(): %s".format(avgSeries.first().toString))
      avgSeries
    }
    new StreamingSeries(filteredData)
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




