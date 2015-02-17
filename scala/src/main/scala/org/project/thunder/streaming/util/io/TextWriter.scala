package org.project.thunder.streaming.util.io

import org.apache.spark.rdd.RDD
import java.io.File

/*** Class for writing an RDD to a text file */

class TextWriter extends SeriesWriter with Serializable {

  def write(index: Option[Int], data: Array[Double], fullFile: String) = {
    printToFile(new File(fullFile ++ ".txt"))(p => {
      // Write out the index if it exists
      index match {
        case Some(idx) => p.print(idx)
        case None => _
      }
      data.foreach(x => p.print("%.6f".format(x)))
      p.print('\n')
    })
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

}

