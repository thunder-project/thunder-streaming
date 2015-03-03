package org.project.thunder.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import org.project.thunder.streaming.util.ThunderStreamingContext

object ExampleLoadStreaming {

  def main(args: Array[String]) {

    val dataPath = args(0)

    val batchTime = args(1).toLong

    val conf = new SparkConf().setAppName("ExampleLoadStreaming").set("spark.default.parallelism", "100")

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val tssc = new ThunderStreamingContext(ssc)

    val data = tssc.loadStreamingSeries(dataPath, inputFormat="binary")

    data.dstream.foreachRDD { rdd =>
      val foo = rdd.filter{case (k, v) => k < 100}.collect()
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
