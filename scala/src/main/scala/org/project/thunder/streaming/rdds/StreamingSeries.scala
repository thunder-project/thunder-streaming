package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

import org.project.thunder.streaming.util.counters.StatUpdater

class StreamingSeries(val dstream: DStream[(List[Int], Array[Double])])
  extends StreamingData[List[Int], Array[Double], StreamingSeries] {

  def seriesStat(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    val output = stats.mapValues(x => Array(x.count, x.mean, x.stdev, x.max, x.min))
    create(output)
  }

  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    val output = stats.mapValues(x => Array(x.mean))
    create(output)
  }

  def print(): Unit = {
    dstream.map{case (k, v) => "(" + k.mkString(",") + ") " + " (" + v.mkString(",") + ")"}.print()
  }

  def create(dstream: DStream[(List[Int], Array[Double])]) = new StreamingSeries(dstream)

}
