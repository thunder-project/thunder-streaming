package org.project.thunder.streaming.util.io

import org.apache.spark.rdd.RDD
import java.io.{PrintWriter, BufferedWriter, FileWriter, File}

/*** Class for writing an RDD to a text file */

class TextWriter extends SeriesWriter with Serializable {

  override def extension = ".txt"

  override def write(data: List[(Int, Array[Double])], file: File, withIndices: Boolean = true) = {
    printToFile(file)(bw => {
      // Write out the index if it exists
      data.foreach(item => {
        if (withIndices) {
          bw.write("%d".format(item._1))
        }
        item._2.foreach(x => bw.write(" %.6f".format(x)))
        bw.write('\n')
      })
    })
  }

  def printToFile(f: File)(op: BufferedWriter => Unit) {
    val bw = new BufferedWriter(new FileWriter(f, true))
    try {
      op(bw)
    } finally {
      bw.close()
    }
  }

}

