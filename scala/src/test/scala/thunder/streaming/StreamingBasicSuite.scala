package thunder.streaming

import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.scalatest.FunSuite
import com.google.common.io.Files
import scala.util.Random
import java.io.File
import org.apache.commons.io.FileUtils

import org.project.thunder.streaming.util.ThunderStreamingContext

/**
 * NOTE: Currently performing all streaming related tests
 * in one suite. I tried moving these exact same tests into separate
 * test suites but it caused several File IO related bugs,
 * still need to track them down.
 */
class StreamingBasicSuite extends FunSuite {

  import thunder.TestUtils._

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")

  test("streaming stats") {

    // set parameters
    val n = 1000 // number of data points per batch
    val m = 10 // number of batches
    val r = 0.05 // noise
    val mean = 3.0 // intercept for linear model
    val variance = 2.0 // coefficients for linear model

    // create test directory and set up streaming data
    val testDir = Files.createTempDir()
    val checkpointDir = Files.createTempDir()
    val ssc = new StreamingContext(conf, Seconds(1))
    val tssc = new ThunderStreamingContext(ssc)
    val data = tssc.loadStreamingSeries(testDir.toString, inputFormat="text")

    // create and train linear model
    val state = data.seriesStats()
    var counter = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    state.dstream.foreachRDD{rdd => counter = rdd.values.first()}
    ssc.checkpoint(checkpointDir.toString)
    ssc.start()

    val rand = new Random(41)

    Thread.sleep(5000)
    for (i <- 0 until m) {
      val samples = Array.fill(n)(rand.nextGaussian()).map(x => x * math.sqrt(variance) + mean)
      val file = new File(testDir, i.toString)
      FileUtils.writeStringToFile(file, "0 " + samples.mkString(" ") + "\n")
      Thread.sleep(Milliseconds(500).milliseconds)
    }
    Thread.sleep(Milliseconds(5000).milliseconds)

    ssc.stop()
    System.clearProperty("spark.driver.port")

    FileUtils.deleteDirectory(testDir)

    // compare estimated parameters to actual
    assertEqual(counter(1), mean, 0.1)
    assertEqual(counter(2), variance, 0.1)
    assertEqual(counter(1), n * m, 0.0001)

  }

}