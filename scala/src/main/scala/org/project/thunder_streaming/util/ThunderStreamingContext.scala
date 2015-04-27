package org.project.thunder_streaming.util

import org.apache.spark.streaming.StreamingContext
import org.project.thunder_streaming.rdds.{StreamingSeriesLoader, StreamingSeries}
import org.zeromq.ZMQ

import org.project.thunder_streaming.rdds.StreamingSeriesLoader

object ThunderStreamingContext {
  val NUM_ZMQ_THREADS = 1
}

class ThunderStreamingContext(val ssc: StreamingContext) {

  val checkpoint_dir = System.getenv("CHECKPOINT")
  println("Checkpoint directory: %s".format(checkpoint_dir))
  ssc.checkpoint(checkpoint_dir)

  val context = ZMQ.context(ThunderStreamingContext.NUM_ZMQ_THREADS)

  def loadStreamingSeries(
      dataPath: String,
      inputFormat: String,
      dataType: String = "short"): StreamingSeries = {

    val loader = new StreamingSeriesLoader(ssc)

    println("inputFormat: %s".format(inputFormat))

    inputFormat match {
      case "text" => loader.fromText(dataPath)
      case "binary" => loader.fromBinary(dataPath, dataType)
      case _ => throw new IllegalArgumentException("Input format must be 'text' or 'binary'")
    }

  }

}
