package org.project.thunder_streaming.rdds

import java.io.{PrintWriter, File}

import org.apache.spark.TaskContext
import org.apache.spark.streaming.{Duration, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.project.thunder_streaming.util.io.TextWriter
import org.project.thunder_streaming.util.counters.StatUpdater
import org.project.thunder_streaming.util.io.BinaryWriter

import spray.json._
import DefaultJsonProtocol._

class StreamingSeries(val dstream: DStream[(Int, Array[Double])],
                      val interval: Duration = Seconds(3000))
  extends StreamingData[Array[Double], StreamingSeries] {

  /** Compute a running estate of several statistics */
  def seriesStats(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    stats.checkpoint(interval)
    val output = stats.mapValues(x => Array(x.count, x.mean, x.stdev, x.max, x.min))
    create(output)
  }

  /** Compute a running estimate of the mean */
  def seriesMean(): StreamingSeries = {
    val stats = dstream.updateStateByKey{StatUpdater.counter}
    stats.checkpoint(interval)
    val output = stats.mapValues(x => Array(x.mean))
    create(output)
  }

  /** Save to output files */
  def save(directory: String, prefix: String): Unit = {

    dstream.foreachRDD{ (rdd, time) =>

      val dirSize = new File(directory).list().length
      val subDir = directory + "/" + dirSize.toString
      new File(subDir).mkdir()

      val writer = new BinaryWriter(subDir, prefix)
      val dims = Map[String, String](
        ("record_size", (if (rdd.count() != 0) rdd.first()._2.length else 0).toString),
        ("dtype", "float64")
      ).toJson

      val writeShard = (context: TaskContext, part: Iterator[(Int, Array[Double])]) => {
        writer.withoutKeys(part, time, context.partitionId)
      }
      rdd.context.runJob(rdd.sortByKey().coalesce(100), writeShard)

      // Write out the dimensions file
      val pw = new PrintWriter(new File(subDir, "dimensions.json"))
      pw.print(dims.toString())
      pw.close()
    }
  }

  /** Print keys and values */
  override def print() {
    dstream.map { case (k, v) => "(" + k.toString + ") " + " (" + v.mkString(",") + ")"}.print()
  }

  override protected def create(dstream: DStream[(Int, Array[Double])]):
  StreamingSeries = new StreamingSeries(dstream)
}
