package org.project.thunder.streaming.rdds

import org.scalatest.FunSuite
import scala.util.Random

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._
import org.apache.spark.streaming.TestSuiteBase

class StreamingSeriesSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 5000

  test("streaming mean") {

    // set parameters
    val numBatches = 10 // number of batches
    val numKeys = 10 // number of keys
    val numPoints = 100 // number of points
    val mean = 3.0
    val variance = 2.0
    val rand = new Random(42)

    val input = RandomStreamGenerator(mean, variance, rand, numBatches, numPoints, numKeys)

    val ssc = setupStreams(input, (inputDStream: DStream[(Int, Array[Double])]) => {
      val series = new StreamingSeries(inputDStream)
      series.seriesMean().dstream
    })

    val output: Seq[Seq[(Int, Array[Double])]] = runStreams(ssc, numBatches, numBatches)

    output.last.foreach{ case (k, v) =>
      assert(Math.abs(v(0) - mean) < 0.1)
    }

  }

  test("streaming stats") {

    // set parameters
    val numBatches = 10 // number of batches
    val numKeys = 10 // number of keys
    val numPoints = 100 // number of points
    val mean = 3.0
    val variance = 2.0
    val rand = new Random(42)

    val input = RandomStreamGenerator(mean, variance, rand, numBatches, numPoints, numKeys)

    val ssc = setupStreams(input, (inputDStream: DStream[(Int, Array[Double])]) => {
      val series = new StreamingSeries(inputDStream)
      series.seriesStats().dstream
    })

    val output: Seq[Seq[(Int, Array[Double])]] = runStreams(ssc, numBatches, numBatches)

    output.last.foreach{ case (k, v) =>
      assert(v(0) == numPoints * numBatches)
      assert(Math.abs(v(1) - mean) < 0.1)
      assert(Math.abs(v(2) - Math.sqrt(variance)) < 0.1)
      assert(v(3) < mean + 3 * variance)
      assert(v(4) > mean - 3 * variance)
    }

  }

  def RandomStreamGenerator(
    mean: Double,
    variance: Double,
    rand: Random,
    numBatches: Int,
    numPoints: Int,
    numKeys: Int
  ): Seq[Seq[(Int, Array[Double])]] = {
    (0 until numBatches).map{
      b => (0 until numKeys).map{
        k => (k, Array.fill(numPoints)(rand.nextGaussian() * Math.sqrt(variance) + mean))
      }
    }
  }

}