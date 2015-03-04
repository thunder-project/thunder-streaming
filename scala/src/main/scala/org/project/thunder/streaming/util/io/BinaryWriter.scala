package org.project.thunder.streaming.util.io

import java.io.{File, FileOutputStream}
import java.nio.{ByteOrder, ByteBuffer}


/*** Class for writing an RDD partition to a flat binary file */

class BinaryWriter(prefix: String, suffix: String)
  extends Writer[Array[Double]](prefix, suffix) with Serializable {

  def extension = ".bin"

  def write(rdd: Iterator[(Int, Array[Double])], file: File, withIndices: Boolean = true): Unit = {
    val fos = new FileOutputStream(file)
    val channel = fos.getChannel
    rdd.foreach(item => {
      val index = item._1
      val arr = item._2
      val bufSize = if (withIndices) 4 + 8 * arr.length else 8 * arr.length
      val bbuf: ByteBuffer = ByteBuffer.allocate(bufSize)
      bbuf.order(ByteOrder.LITTLE_ENDIAN)
      if (withIndices) {
        bbuf.putInt(index)
      }
      arr.foreach(bbuf.putDouble)
      bbuf.flip()
      while (bbuf.hasRemaining) {
        channel.write(bbuf)
      }
    })
    fos.flush()
    fos.close()
  }
}
