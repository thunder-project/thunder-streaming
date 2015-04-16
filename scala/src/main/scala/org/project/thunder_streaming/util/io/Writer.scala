package org.project.thunder_streaming.util.io

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
abstract class Writer[V](directory: String, prefix: String) extends Serializable {

  def write(part: Iterator[(Int, V)], file: File, withKeys: Boolean = false): Unit

  def extension: String

  private def seriesFile(time: Time, id: Int): File = {
    new File(directory ++ File.separator ++ prefix ++ "-" ++ id.toString ++ "-"
      ++ time.toString.split(" ")(0) ++ extension)
  }

  def withoutKeys(part: Iterator[(Int, V)], time: Time, id: Int) {
    val f = seriesFile(time, id)
    write(part, f)
  }

  def withKeys(part: Iterator[(Int, V)], time: Time, id: Int) {
    val f = seriesFile(time, id)
    write(part, f, withKeys=true)
  }
}
