package org.project.thunder.streaming.util.launch

/**
 * Created by Andrew on 2/12/15.
 */
object Launcher {

  def main(args: Array[String]): Unit = {

    // TODO Better arg parsing (need to do more than just get the XML file name)
    // Just assume that the first argument is the XML file path for now
    val filePath = args(0)

    // TODO launch/start the analyses
    val runSpec = new RunSpecification(filePath)
    runSpec.runAnalyses()

    // TODO cleanup

  }

}
