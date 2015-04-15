package org.project.thunder.streaming.util.counters

import org.apache.spark.util.StatCounter

object StatUpdater {

  /** Update a simple stat counter */
  def counter = (values: Seq[Array[Double]], state: Option[StatCounter]) => {
    val updatedState = state.getOrElse(new StatCounter())
    val vec = values.flatten
    Some(updatedState.merge(vec))
  }

  def featuresToBins(features: Array[Double], leftEdges: Array[Double]): Array[Int] = {
    val partialBins = leftEdges.sorted.zipWithIndex.map{ case (edge, idx) => (edge, idx) }
    // A final right-most edge is added, which also maps to bin 0
    val allBins = partialBins :+ (Double.MaxValue, 0)
    for (feature <- features; bin <- allBins; if feature < bin._1 ) yield bin._2
  }

  /** Update a combination of stat counter and stat counter array */
  def mixed = (
      input: Seq[Array[Double]],
      state: Option[StatCounterMixed],
      features: Array[Double],
      leftEdges: Array[Double]) => {

    // N left bin edges correspond to N + 1 bins total
    val numBins = leftEdges.size + 1
    val updatedState = state.getOrElse(StatCounterMixed(new StatCounter(), new StatCounterArray(numBins)))

    val values = input.foldLeft(Array[Double]()) { (acc, i) => acc ++ i}
    val currentCount = values.size
    val n = features.size

    if ((currentCount != 0) && (n != 0)) {

      // For each value in the feature vector, compute its bin
      val binVector = featuresToBins(features, leftEdges)

      // group new data by the features
      val pairs = binVector.zip(values)
      val grouped = pairs.groupBy{case (k,v) => k}

      // get data from each bin, ignoring the 0 bin
      val binnedData = Range(1,numBins).map{ ind => if (grouped.contains(ind)) {
        grouped(ind).map{ case (k,v) => v}
      } else {
        Array[Double]()
      }
      }.toArray

      // get all data, ignoring the 0 bin
      val all = pairs.filter{case (k,v) => k != 0}.map{case (k,v) => v}

      // update the combined stat counter
      updatedState.counter.merge(all)

      // update the binned stat counter array
      updatedState.counterArray.merge(binnedData)

    }

    Some(updatedState)
  }


}
