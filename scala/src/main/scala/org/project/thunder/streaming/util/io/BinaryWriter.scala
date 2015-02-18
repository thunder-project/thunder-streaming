package org.project.thunder.streaming.util.io

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

/*** Class for writing an RDD to a flat binary file */

class BinaryWriter extends SeriesWriter with Serializable {

  override def extension = ".bin"

  override def write(data: List[(Int, Array[Double])], file: File, withIndices: Boolean = true): Unit = {
    val fos = new FileOutputStream(file)
    val channel = fos.getChannel
    data.foreach(item => {
      val index = item._1
      val arr = item._2
      val bufSize = if (withIndices) (4 + 8 * arr.length) else 8 * arr.length
      val bbuf: ByteBuffer = ByteBuffer.allocate(bufSize)
      if (withIndices) {
        bbuf.putInt(0, index)
        (0 until arr.length).foreach(i => bbuf.putDouble(i + 1, arr(i)))
      } else {
          (0 until arr.length).foreach(i => bbuf.putDouble(i, arr(i)))
      }
      while (bbuf.hasRemaining) {
        channel.write(bbuf)
      }
    })
    fos.flush()
    fos.close()
  }
}
