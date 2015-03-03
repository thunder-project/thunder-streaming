package org.project.thunder.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.project.thunder.streaming.util.ThunderStreamingContext
import org.project.thunder.streaming.util.counters.StatUpdater

object ExampleLoadStreaming {

  def main(args: Array[String]) {

    val dataPath = args(0)

    val batchTime = args(1).toLong

    val conf = new SparkConf().setAppName("ExampleLoadStreaming").set("spark.default.parallelism", "320")

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    ssc.sparkContext.hadoopConfiguration.setLong("fs.local.block.size", 1 * 1024 * 1024)

    ssc.checkpoint("/tier2/freeman/checkpoint")

    val tssc = new ThunderStreamingContext(ssc)

    val data = tssc.loadStreamingSeries(dataPath, inputFormat="binary")

    data.dstream.checkpoint(Seconds(100))

    val stats = data.dstream.updateStateByKey(StatUpdater.counter)

    stats.count().print()

//    means.dstream.foreachRDD { rdd =>
//      val foo = rdd.filter{case (k, v) => k < 1000}.collect()
//    }

    ssc.start()
    ssc.awaitTermination()

  }
}
