package org.project.thunder.streaming.examples

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.project.thunder.streaming.util.io.Parser
import org.project.thunder.streaming.util.io.hadoop.FixedLengthBinaryInputFormat

object ExampleLoadStreaming {

  def main(args: Array[String]) {

    val dataPath = args(0)

    val batchTime = args(1).toLong

    val conf = new SparkConf()
      .setJars(List("target/scala-2.10/thunder-streaming_2.10-0.1.0_dev.jar"))
      .set("spark.executor.memory", "100G")
      .set("spark.default.parallelism", "100")
      .setAppName("ExampleLoadStreaming")

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val parser = new Parser("short")

    val lines = ssc.fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](dataPath)

    val dstream = lines.map{ case (k, v) => (k.get().toInt, parser.get(v.getBytes))}

    dstream.count().print()

    ssc.start()
    ssc.awaitTermination()

  }
}
