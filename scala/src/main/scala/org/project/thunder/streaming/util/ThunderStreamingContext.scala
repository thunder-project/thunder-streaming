package org.project.thunder.streaming.util

import org.apache.spark.streaming.StreamingContext
import org.project.thunder.streaming.rdds.{StreamingSeries, StreamingSeriesLoader}

class ThunderStreamingContext(ssc: StreamingContext) {

  ssc.checkpoint(System.getenv("CHECKPOINT"))

  def loadStreamingSeries(
      dataPath: String,
      inputFormat: String,
      nkeys: Option[Int] = None,
      nvalues: Option[Int] = None,
      keyType: Option[String] = None,
      valueType: Option[String] = None): StreamingSeries = {

    val loader = new StreamingSeriesLoader(ssc)

    inputFormat match {
      case "text" => loader.fromText(dataPath, nkeys.get)
      case "binary" => loader.fromBinary(dataPath, nkeys.get, nvalues.get, keyType.get, valueType.get)
      case _ => throw new IllegalArgumentException("Input format must be 'text' or 'binary'")
    }

  }

}
