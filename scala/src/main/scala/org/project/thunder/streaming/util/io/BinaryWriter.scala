package org.project.thunder.streaming.util.io

import org.apache.spark.rdd.RDD
import java.io.FileOutputStream

/*** Class for writing an RDD to a flat binary file */

class BinaryWriter extends SeriesWriter with Serializable {

  override def write(data: List[(Int, Array[Double])], fullFile: String): Unit = {
    val file = new FileOutputStream(fullFile)
    val channel = file.getChannel
    val bbuf = java.nio.ByteBuffer.allocate(12*data.length)
    // TODO: finish this (how should keys/values be stored in the binary file?
  }

  def write(rdd: RDD[Double], fullFile: String) {
    val out = rdd.collect()
    val file = new FileOutputStream(fullFile ++ ".bin")
    val channel = file.getChannel
    val bbuf = java.nio.ByteBuffer.allocate(8*out.length)
    bbuf.asDoubleBuffer.put(java.nio.DoubleBuffer.wrap(out))

    while(bbuf.hasRemaining) {
      channel.write(bbuf)
    }
  }

}
