package org.project.thunder.streaming.util

import org.apache.spark.streaming.StreamingContext
import org.project.thunder.streaming.rdds.{StreamingSeries, StreamingSeriesLoader}

class ThunderStreamingContext(ssc: StreamingContext) {

  ssc.checkpoint(System.getenv("CHECKPOINT"))

  def loadStreamingSeries(
      dataPath: String,
      inputFormat: String,
      nKeys: Int = 1,
      dataType: String = "short"): StreamingSeries = {

    val loader = new StreamingSeriesLoader(ssc)

    inputFormat match {
      case "text" => loader.fromText(dataPath, nKeys)
      case "binary" => loader.fromBinary(dataPath, nKeys, dataType)
      case _ => throw new IllegalArgumentException("Input format must be 'text' or 'binary'")
    }

  }

}
