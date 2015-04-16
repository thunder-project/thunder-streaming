package org.project.thunder_streaming.util.counters

import org.apache.spark.util.StatCounter

/**
 * A class for describing the combination of a StatCounter (for simple statistics)
 * and a StatCounterArray (for a set of statistics). Typically the StatCounter
 * will contain a global set of statistics, and the StatCounterArray will contain
 * a set of statistics within each of several groups, and the provided methods
 * will compute statistics relating these two quantities.
 */
case class StatCounterMixed (var counter: StatCounter, var counterArray: StatCounterArray) {

  def r2: Double = {
    1 - counterArray.combinedVariance / counter.variance
  }

  def weightedMean(featureValues: Array[Double]): Double = {
    val means = counterArray.mean
    val pos = means.map(x => (x - counter.mean) / counter.mean).map{x => if (x < 0) 0 else x}
    val weights = pos.map(x => x / pos.sum)
    weights.zip(featureValues).map{case (x,y) => x * y}.sum
  }

}