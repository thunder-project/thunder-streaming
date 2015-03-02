package org.project.thunder.streaming.util.io

import java.io.File
import org.apache.spark.streaming.Time

/**
 * Generic writer class for writing the contents of StreamingSeries to disk.
 * Separate methods for handing data with or without keys.
 *
 * This class should be extended by defining a write method specific
 * to a particular output type (see e.g. TextWriter, BinaryWriter).
 *
 */
abstract class SeriesWriter {

  def write(data: List[(Int, Array[Double])], file: File, withIndices: Boolean = true): Unit

  def extension: String

  private def seriesFile(directory:  String, fileName: String, time: Time): File = {
    new File(directory ++ File.separator ++ fileName ++ "-" ++ time.toString.split(" ")(0) ++ extension)
  }

  def withoutKeys(data: List[(Int, Array[Double])], time: Time, directory: String, fileName: String) {
    if (data.length > 0) {
      val f = seriesFile(directory, fileName, time)
      write(data, f, false)
    }
  }

  def withKeys(data: List[(Int, Array[Double])], time: Time, directory: String, fileName: String) {
    if (data.length > 0) {
      val f = seriesFile(directory, fileName, time)
      write(data, f)
    }
  }
}
