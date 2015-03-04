package org.project.thunder.streaming.rdds

import org.apache.spark.TaskContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

import org.project.thunder.streaming.util.counters.StatUpdater
import org.project.thunder.streaming.util.io.{BinaryWriter, TextWriter}

class StreamingSeries(val dstream: DStream[(Int, Array[Double])])
  extends StreamingData[Array[Double], StreamingSeries] {

  /** Compute a running estate of several statistics */
  def seriesStats(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    stats.checkpoint(Seconds(System.getenv("CHECKPOINT_INTERVAL").toInt))
    val output = stats.mapValues(x => Array(x.count, x.mean, x.stdev, x.max, x.min))
    create(output)
  }

  /** Compute a running estimate of the mean */
  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    stats.checkpoint(Seconds(System.getenv("CHECKPOINT_INTERVAL").toInt))
    val output = stats.mapValues(x => Array(x.mean))
    create(output)
  }

  /** Save to output files */
  def save(directory: String, prefix: String): Unit = {
    val writer = new BinaryWriter(directory, prefix)
    dstream.foreachRDD{ (rdd, time) =>
      val writeShard = (context: TaskContext, part: Iterator[(Int, Array[Double])]) => {
        writer.withKeys(part, time, context.partitionId)
      }
      rdd.context.runJob(rdd, writeShard)
    }
  }

  /** Print keys and values */
  override def print() {
    dstream.map { case (k, v) => "(" + k.toString + ") " + " (" + v.mkString(",") + ")"}.print()
  }

  override protected def create(dstream: DStream[(Int, Array[Double])]):
    StreamingSeries = new StreamingSeries(dstream)
}
