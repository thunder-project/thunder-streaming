package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter
import org.apache.spark.streaming.StreamingContext._

class StreamingSeries(dstream: DStream[(Array[Int], Array[Double])]) {


  /** State updating function that updates the statistics for each key. */
  val runningStats = (values: Seq[Array[Double]], state: Option[StatCounter]) => {
    val updatedState = state.getOrElse(new StatCounter())
    val vec = values.flatten
    Some(updatedState.merge(vec))
  }

  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{runningStats}
    val output = stats.mapValues(x => Array(x.mean))
    new StreamingSeries(output)
  }

  def print() {
    dstream.mapValues(x => x.mkString(", ")).print()
  }

}
