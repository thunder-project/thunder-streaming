package org.project.thunder.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import org.project.thunder.streaming.util.ThunderStreamingContext

object ExampleLoadStreaming {

  def main(args: Array[String]) {

    val master = args(0)

    val dataPath = args(1)

    val batchTime = args(2).toLong

    val conf = new SparkConf().setMaster(master).setAppName("ExampleLoadStreaming")

    if (!master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
        .setJars(List("target/scala-2.10/thunder_2.10-0.1.0.jar"))
        .set("spark.executor.memory", "100G")
    }

    val ssc = new StreamingContext(conf, Seconds(batchTime))
    val tssc = new ThunderStreamingContext(ssc)

    val data = tssc.loadStreamingSeries(dataPath, inputFormat="text", nkeys=Some(1))

    data.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
