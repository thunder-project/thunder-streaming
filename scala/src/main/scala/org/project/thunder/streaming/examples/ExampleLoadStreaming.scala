package org.project.thunder.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
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

    val stats = data.seriesMean()
    //val stats = data.dstream.updateStateByKey(StatUpdater.counter)

    //val stats = data.dstream.reduceByKey{case (v1, v2) => Array(v1(0) + v2(0))}

    stats.dstream.checkpoint(Seconds(batchTime * 1000))

    stats.filter{case (k, v) => k < 100}.print()

    stats.dstream.foreachRDD{ rdd =>
      val out = rdd.sortByKey().filter{case (k, v) => k < 100}
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
