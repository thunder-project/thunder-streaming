package org.project.thunder.streaming.outputs

import org.apache.spark.streaming.Time
import org.project.thunder.streaming.util.io.{SeriesWriter, TextWriter, BinaryWriter}

/**
 *
 */
class SeriesFileOutput(override val params: Map[String, String]) extends Output[List[(Int, Array[Double])]](params) {

  override def handleResult(data: List[(Int, Array[Double])], time: Time): Unit = {
    // Try to find a writer for the given parameters
    val maybeMethods = for {
      maybeDir <- params.get(SeriesFileOutput.DIR_KEY)
      maybePrefix <- params.get(SeriesFileOutput.PREFIX_KEY)
      maybeFormat <- params.get(SeriesFileOutput.FORMAT_KEY)
      maybeWriter <- SeriesFileOutput.formatToWriter.get(maybeFormat)
      maybeWithKeys <- params.get(SeriesFileOutput.INCLUDE_KEYS_KEY)
      maybeWriterMethod <- SeriesFileOutput.methodForKeyParam(maybeWriter, maybeWithKeys)
    } yield maybeWriterMethod(data, time, maybeDir, maybePrefix)
  }
}

object SeriesFileOutput {
  final val DIR_KEY = "directory"
  final val PREFIX_KEY = "prefix"
  final val FORMAT_KEY = "format"
  final val INCLUDE_KEYS_KEY = "include_keys"

  // All supported file output formats go here:
  final val formatToWriter: Map[String, SeriesWriter] = Map(
    ("text", new TextWriter()),
    ("binary", new BinaryWriter())
  )

  type WriterMethodType = (List[(Int, Array[Double])], Time, String, String) => Unit

  // Get the writer method based on the keys parameter
  def methodForKeyParam(writer: SeriesWriter, keyParam: String): Option[WriterMethodType] = {
    Map[String, WriterMethodType](
      ("true", writer.withKeys(_, _, _, _)),
      ("false" ,writer.withoutKeys(_, _, _, _))
    ).get(keyParam)
  }
}
