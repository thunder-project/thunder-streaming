package org.project.thunder_streaming.util.counters

import org.apache.spark.util.StatCounter

object StatUpdater {

  /** Update a simple stat counter */
  def counter = (values: Seq[Array[Double]], state: Option[StatCounter]) => {
    val updatedState = state.getOrElse(new StatCounter())
    val vec = values.flatten
    Some(updatedState.merge(vec))
  }

  /** Update a combination of stat counter and stat counter array */
  def mixed = (
      input: Seq[Array[Double]],
      state: Option[StatCounterMixed],
      numBins: Int,
      binVector: Array[Int]) => {

    val updatedState = state.getOrElse(StatCounterMixed(new StatCounter(), new StatCounterArray(numBins)))

    val values = input.foldLeft(Array[Double]()) { (acc, i) => acc ++ i}
    val currentCount = values.size
    val n = binVector.size

    if ((currentCount != 0) && (n != 0)) {

      // group new data by the features
      val pairs = binVector.zip(values)
      val grouped = pairs.groupBy{case (k,v) => k}

      // get data from each bin, ignoring the 0 bin
      val binnedData = Range(1, numBins + 1).map{ ind => if (grouped.contains(ind)) {
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
