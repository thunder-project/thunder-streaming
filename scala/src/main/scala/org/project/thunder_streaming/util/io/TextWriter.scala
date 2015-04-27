package org.project.thunder_streaming.util.io

import java.io.{BufferedWriter, FileWriter, File}

/*** Class for writing an RDD partition to a text file */

class TextWriter(directory: String, prefix: String)
  extends Writer[Array[Double]](directory, prefix) {

  def extension = ".txt"

  def write(part: Iterator[(Int, Array[Double])], file: File, withKeys: Boolean = true) = {
    printToFile(file)(bw => {
      // Write out the index if it exists
      part.foreach(item => {
        if (withKeys) {
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

