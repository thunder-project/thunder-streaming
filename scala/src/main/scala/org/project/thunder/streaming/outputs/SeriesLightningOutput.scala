package org.project.thunder.streaming.outputs

/**
 * Created by Andrew on 2/13/15.
 */
class SeriesLightningOutput(override val params: Map[String, String]) extends AnalysisOutput[List[(Int, Array[Double])]](params) {

  override def handleResult(data: List[(Int, Array[Double])]): Unit =  {

  }
}
