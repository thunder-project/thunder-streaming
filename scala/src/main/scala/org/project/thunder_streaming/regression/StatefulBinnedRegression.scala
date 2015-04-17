
package org.project.thunder_streaming.regression

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.project.thunder_streaming.util.counters.StatCounterMixed
import org.project.thunder_streaming.rdds.StreamingSeries
import org.project.thunder_streaming.util.counters.{StatUpdater, StatCounterMixed}


/**
 * Stateful binned statistics
 */
class StatefulBinnedRegression (
    var featureKey: Int,
    var leftEdges: Array[Double])
  extends Serializable with Logging {

  def this() = this(0, Array(0.0))

  /** Set which indices that correspond to features. */
  def setFeatureKey(featureKey: Int): StatefulBinnedRegression = {
    this.featureKey = featureKey
    this
  }

  /** Set the values associated with the to features. */
  def setLeftEdges(leftEdges: Array[Double]): StatefulBinnedRegression = {
    this.leftEdges = leftEdges
    this
  }

  def featuresToBins(features: Array[Double], leftEdges: Array[Double]): Array[Int] = {
    val partialBins = leftEdges.sorted.zipWithIndex
    // A final right-most edge is added, which maps to bin N
    val allBins = partialBins :+ (Double.MaxValue, partialBins.size)
    val minBins = features.map{ f => (f, allBins.filter{ case (n, idx) => f < n }) }
                          .map{ case (f, bins) => bins.map{ case (n, idx) => idx }.min }
    println("minBins: %s".format(minBins.mkString(",")))
    minBins
  }

  def fit(data: StreamingSeries): DStream[(Int, StatCounterMixed)] = {

    var features = Array[Double]()

    // N left bin edges correspond to N - 1 bins of interest total (not including the 0 bin and the
    // Nth bin)
    val numBins = leftEdges.size - 1

    // For each value in the feature vector, compute its bin
    var binVector = featuresToBins(features, leftEdges)

    // extract the bin labels
    data.dstream.filter{case (k, _) => featureKey == k}.foreachRDD{rdd =>
      val batchFeatures = rdd.values.collect().flatten
      features = batchFeatures.size match {
        case 0 => Array[Double]()
        case _ => batchFeatures
      }
      println("features: %s".format(features.mkString(",")))
      binVector = featuresToBins(features, leftEdges)
    }

    // update the stats for each key
    data.dstream.updateStateByKey{(x, y) => StatUpdater.mixed(x, y, numBins, binVector)}

  }

}

/**
 * Top-level methods for calling Stateful Binned Stats.
 */
object StatefulBinnedRegression {

  /**
   * Compute running statistics on keyed data points in bins.
   * For each key, statistics are computed within each of several bins
   * specified by auxiliary data passed as a special key.
   * We assume that in each batch of streaming data we receive
   * an array of doubles for each data key, and an array of integer indices
   * for the bin key. We use a StatCounterArray on
   * each key to update the statistics within each bin.
   *
   * @param input StreamingSeries with keyed data
   * @return StreamingSeries with statistics
   */
  def runToSeries(
    input: StreamingSeries,
    featureKey: Int,
    leftEdges: Array[Double]): StreamingSeries =
  {
    val output = new StatefulBinnedRegression()
      .setFeatureKey(featureKey)
      .setLeftEdges(leftEdges)
      .fit(input)
      .map{ case (idx, model) => (idx, Array[Double](model.weightedMean(leftEdges))) }

    new StreamingSeries(output)
  }

  def run(
     input: StreamingSeries,
     featureKey: Int,
     leftEdges: Array[Double]): DStream[(Int, StatCounterMixed)] = {

    new StatefulBinnedRegression()
      .setFeatureKey(featureKey)
      .setLeftEdges(leftEdges)
      .fit(input)
  }

  def binCenters(edges: Array[Double]): Array[Double] = {
    val sortedEdges = edges.sorted
    val binPairs = sortedEdges.zip(sortedEdges.tail)
    binPairs.map{ case (b1, b2) => b1 + ((b1 - b2) / 2.0)}
  }
}
