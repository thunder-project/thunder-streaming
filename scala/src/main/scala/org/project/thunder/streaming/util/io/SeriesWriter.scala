package org.project.thunder.streaming.util.io

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import java.util.Calendar

import org.project.thunder.streaming.rdds.StreamingData
import org.project.thunder.streaming.rdds.StreamingSeries.SeriesDataType

/**
 * Generic writer class for writing the contents of StreamingSeries to disk.
 * Separate methods for handing data with or without keys.
 *
 * This class should be extended by defining a write method specific
 * to a particular output type (see e.g. TextWriter, BinaryWriter).
 *
 */
abstract class SeriesWriter {

  def write(index: Option[Int], data: Array[Double], fullFile: String): Unit

  def withoutKeys(data: SeriesDataType, time: Time, directory: String, fileName: String) {
    data.foreach({
      case (index, arr) => write(Some(index), arr , SeriesWriter.seriesFileName(directory, fileName, time))
    })
  }

  def withKeys(data: SeriesDataType, time: Time, directory: String, fileName: String) {
    data.foreach({
      case (index, arr) => write(None, arr, SeriesWriter.seriesFileName(directory, fileName, time))
    })
  }
}

object SeriesWriter {
  def seriesFileName(directory:  String, fileName: String, time: Time): String = {
    directory ++ File.separator ++ fileName ++ "-" ++ time.toString
  }
}