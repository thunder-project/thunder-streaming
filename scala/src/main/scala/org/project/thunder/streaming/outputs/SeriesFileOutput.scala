package org.project.thunder.streaming.outputs

import org.project.thunder.streaming.util.io.{Writer, TextWriter, BinaryWriter}

/**
 *
 */
class SeriesFileOutput(override val params: Map[String, String]) extends AnalysisOutput[List[(Int, Array[Double])]](params) {

  override def handleResult(data: List[(Int, Array[Double])]): Unit =  {
    val path = params.get(SeriesFileOutput.PATH_KEY)
    // Try to find a writer for the given format
    val maybeWriter = for {
      maybeFormat <- params.get(SeriesFileOutput.FORMAT_KEY)
      maybeWriter <- SeriesFileOutput.formatToWriter.get(maybeFormat)
    } yield maybeWriter
    // Using foreach because each writer performs I/O as a side-effect
    maybeWriter.foreach(writer => {
      writer.write(data)
    })

    println("In SeriesFileOutput: %s".format(data.map(pair => (pair._1, pair._2.toList))))
  }
}

object SeriesFileOutput {
  final val PATH_KEY = "path"
  final val FORMAT_KEY = "format"

  // All supported file output formats go here:
  val formatToWriter: Map[String, Writer] = Map({
    "text" -> new TextWriter()
    "binary" -> new BinaryWriter()
  })
}
