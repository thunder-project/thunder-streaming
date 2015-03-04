package org.project.thunder.streaming.util.io

import java.io.File
import org.apache.spark.streaming.Time

/**
 * Generic writer class for writing the contents of StreamingData to disk.
 * Separate methods for handing data with or without keys.
 *
 * This class should be extended by defining a write method specific
 * to a particular output type (see e.g. TextWriter, BinaryWriter).
 *
 */
abstract class Writer[V](directory: String, fileName: String) {

  def write(rdd: Iterator[(Int, V)], file: File, withKeys: Boolean = false): Unit

  def extension: String

  private def seriesFile(time: Time): File = {
    new File(directory ++ File.separator ++ fileName ++ "-" ++ time.toString.split(" ")(0) ++ extension)
  }

  def withoutKeys(rdd: Iterator[(Int, V)], time: Time) {
    if (rdd.length > 0) {
      val f = seriesFile(time)
      write(rdd, f)
    }
  }

  def withKeys(rdd: Iterator[(Int, V)], time: Time) {
    if (rdd.length > 0) {
      val f = seriesFile(time)
      write(rdd, f, withKeys=true)
    }
  }
}
