package org.project.thunder.streaming.util.launch

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.project.thunder.streaming.util.ThunderStreamingContext

/**
 * Created by Andrew on 2/12/15.
 */
object Launcher {

  def main(args: Array[String]): Unit = {

    // TODO Better arg parsing (need to do more than just get the XML file name)

    // For now, the script takes NO arguments, and necessary parameters are passed in as environment variables
    val master = System.getenv("MASTER")

    val batchTime = System.getenv("BATCH_TIME").toInt

    val filePath = System.getenv("CONFIG_FILE_PATH")

    println("filePath: %s".format(filePath))

    val conf = new SparkConf().setMaster(master).setAppName("ExampleLoadStreaming")

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val tssc = new ThunderStreamingContext(ssc)

    // TODO launch/start the analyses
    val runSpec = new AnalysisManager(tssc, filePath)
    runSpec.register()

    ssc.start()
    ssc.awaitTermination()

    // TODO cleanup
  }

}
