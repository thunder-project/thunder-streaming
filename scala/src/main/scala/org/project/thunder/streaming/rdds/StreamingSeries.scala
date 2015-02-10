package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter
import org.apache.spark.streaming.StreamingContext._

class StreamingSeries(dstream: DStream[(List[Int], Array[Double])]) {

  /** State updating function that updates statistics for each key. */
  val runningStats = (values: Seq[Array[Double]], state: Option[StatCounter]) => {
    val updatedState = state.getOrElse(new StatCounter())
    val vec = values.flatten
    Some(updatedState.merge(vec))
  }

  def seriesStat(): StreamingSeries = {
    val stats = dstream.updateStateByKey{runningStats}
    val output = stats.mapValues(x => Array(x.count, x.mean, x.stdev, x.max, x.min))
    new StreamingSeries(output)
  }

  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{runningStats}
    val output = stats.mapValues(x => Array(x.mean))
    new StreamingSeries(output)
  }

  def print() {
    dstream.map{ case (k, v) => "(" + k.mkString(",") + "), " + "(" + v.mkString(",") + ")"}.print()
  }

}
