package org.project.thunder.streaming.outputs

/**
 * Created by Andrew on 2/13/15.
 */
class SeriesFileOutput(params: Map[String, String]) extends AnalysisOutput[List[(Int, Array[Double])]] {

  override def handleResult(data: List[(Int, Array[Double])]): Unit =  {
    val path = params.get(SeriesFileOutput.PATH_KEY)

    println("In SeriesFileOutput: %s".format(data.toString))

  }
}

object SeriesFileOutput {
  final val PATH_KEY = "path"
}
