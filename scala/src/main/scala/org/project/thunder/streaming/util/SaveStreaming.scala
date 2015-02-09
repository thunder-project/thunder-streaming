package org.project.thunder.streaming.util

import org.apache.spark.streaming.dstream.DStream

import org.project.thunder.streaming.util.io.{TextWriter, BinaryWriter}

/** Object with methods for saving results from a DStream */

object SaveStreaming {

  def asTextWithKeys(data: DStream[(Int, Array[Double])], directory: String, fileName: Seq[String]) =
    new TextWriter().withKeys(data, directory, fileName)

  def asBinaryWithKeys(data: DStream[(Int, Array[Double])], directory: String, fileName: Seq[String]) =
    new BinaryWriter().withKeys(data, directory, fileName)

}