
package org.project.thunder.streaming.regression

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import org.project.thunder.streaming.rdds.StreamingSeries
import org.project.thunder.streaming.util.counters.{StatCounterMixed, StatUpdater}


/**
 * Stateful binned statistics
 */
class StatefulBinnedStats (
    var featureKey: Int,
    var nfeatures: Int)
  extends Serializable with Logging {

  def this() = this(0, 1)

  /** Set which indices that correspond to features. */
  def setFeatureKey(featureKey: Int): StatefulBinnedStats = {
    this.featureKey = featureKey
    this
  }

  /** Set the values associated with the to features. */
  def setFeatureCount(nfeatures: Int): StatefulBinnedStats = {
    this.nfeatures = nfeatures
    this
  }

  def run(data: StreamingSeries): DStream[(Int, StatCounterMixed)] = {

    var features = Array[Double]()

    // extract the bin labels
    data.dstream.filter{case (k, _) => featureKey == k}.foreachRDD{rdd =>
      val batchFeatures = rdd.values.collect().flatten
      features = batchFeatures.size match {
        case 0 => Array[Double]()
        case _ => batchFeatures
      }
    }

    // update the stats for each key
    data.dstream.updateStateByKey{(x, y) => StatUpdater.mixed(x, y, features, nfeatures)}

  }

}

/**
 * Top-level methods for calling Stateful Binned Stats.
 */
object StatefulBinnedStats {

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
  def fit(
    input: StreamingSeries,
    featureKey: Int,
    featureCount: Int,
    featureValues: Array[Double]): StreamingSeries =
  {
    val output = new StatefulBinnedStats()
      .setFeatureKey(featureKey)
      .setFeatureCount(featureCount)
      .run(input)
      .mapValues(x => Array(x.r2, x.weightedMean(featureValues)))

    new StreamingSeries(output)
  }

}
