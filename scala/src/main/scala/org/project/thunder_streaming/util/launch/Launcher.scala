package org.project.thunder_streaming.util.launch

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.project.thunder_streaming.util.ThunderStreamingContext

object Launcher {

  def main(args: Array[String]): Unit = {

    // TODO Better arg parsing (need to do more than just get the XML file name)

    // For now, the script takes NO arguments, and necessary parameters are passed in as environment variables
    val master = System.getenv("MASTER")

    val batchTime = System.getenv("BATCH_TIME").toInt

    val filePath = System.getenv("CONFIG_FILE_PATH")

    println("filePath: %s".format(filePath))

    val conf = new SparkConf().setMaster(master)
      .setAppName("ExampleLoadStreaming")
      .set("spark.default.parallelism", System.getenv("PARALLELISM"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", System.getenv("EXECUTOR_MEMORY"))

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    ssc.sparkContext.hadoopConfiguration.setLong("fs.local.block.size",
      System.getenv("HADOOP_BLOCK_SIZE").toInt * 1024 * 1024)

    val tssc = new ThunderStreamingContext(ssc)

    // TODO launch/start the analyses
    val runSpec = new AnalysisManager(tssc, filePath)
    runSpec.load(tssc, filePath).foreach(a => a.process())

    ssc.start()
    ssc.awaitTermination()

    // TODO cleanup (?)
  }

}
