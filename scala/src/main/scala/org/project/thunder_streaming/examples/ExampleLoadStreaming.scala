package org.project.thunder_streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.project.thunder_streaming.util.ThunderStreamingContext

object ExampleLoadStreaming {

  def main(args: Array[String]) {

    val master = args(0)

    val dataPath = args(1)

    val batchTime = args(2).toLong

    val conf = new SparkConf().setMaster(master).setAppName("ExampleLoadStreaming")

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val tssc = new ThunderStreamingContext(ssc)

    val data = tssc.loadStreamingSeries(dataPath, inputFormat="binary")

    data.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
