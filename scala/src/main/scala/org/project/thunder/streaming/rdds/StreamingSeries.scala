package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

import org.project.thunder.streaming.util.counters.StatUpdater
import org.project.thunder.streaming.util.io.{BinaryWriter, TextWriter}

class StreamingSeries(val dstream: DStream[(Int, Array[Double])])
  extends StreamingData[Int, Array[Double], StreamingSeries] {

  /** Compute a running estate of several statistics */
  def seriesStat(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    val output = stats.mapValues(x => Array(x.count, x.mean, x.stdev, x.max, x.min))
    create(output)
  }

  /** Compute a running estimate of the mean */
  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    val output = stats.mapValues(x => Array(x.mean))
    create(output)
  }

  /** Save data from each batch as binary files */
  def saveAsBinary(directory: String, fileName: Seq[String]) = {
    new BinaryWriter().withKeys(dstream, directory, fileName)
  }

  /** Save data from each batch as text files */
  def saveAsText(directory: String, fileName: Seq[String]) = {
    new TextWriter().withKeys(dstream, directory, fileName)
  }

  /** Print keys and values */
  override def print() {
    dstream.map{case (k, v) => "(" + k.toString + ") " + " (" + v.mkString(",") + ")"}.print()
  }

  override def create(dstream: DStream[(Int, Array[Double])]) = new StreamingSeries(dstream)

}
