package org.project.thunder.streaming.regression

import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.project.thunder.streaming.rdds.StreamingSeries
import org.scalatest.FunSuite

import scala.util.Random

import org.project.thunder.streaming.util.TestUtils._

class StatefulLinearRegressionSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 5000

  test("weight accuracy (single feature)") {

    val numBatches = 10
    val numPoints = 2
    val numKeys = 5
    val intercept = 1.0 // intercept for linear model
    val weights = Array(2.0) // coefficients for linear model
    val noise = 0.5 // noise level
    val rand = new Random(42)

    val input = RegressionStreamGenerator(
      intercept, weights, rand, numBatches, numPoints, numKeys, noise)

    val model = new StatefulLinearRegression()
      .setFeatureKeys(Array(numKeys + 1))
      .setSelectedKeys(Array(numKeys + 1))

    val ssc = setupStreams(input, (inputDStream: DStream[(Int, Array[Double])]) => {
      val series = new StreamingSeries(inputDStream)
      model.fit(series)
    })

    val output: Seq[Seq[(Int, FittedModel)]] = runStreams(ssc, numBatches, numBatches)

    output.last.foreach{ case (k, v) =>
      assertEqual(v.weights, weights, 0.1)
    }

  }

  test("weight accuracy (multiple features)") {

    // are estimated weights correct

  }

  test("convergence") {

    // is the error in the betas on the first batch higher than on the final batch

  }

  test("r2") {

    // is it perfect when you have no noise
    // is it lower when you have more noise

  }

  test("key selection") {

    // are the right features used?
    // is it ok to have different sets of featureKeys and selectedKeys

  }

  def RegressionStreamGenerator(
    intercept: Double,
    weights: Array[Double],
    rand: Random,
    numBatches: Int,
    numPoints: Int,
    numKeys: Int,
    noise: Double
  ): Seq[Seq[(Int, Array[Double])]] = {
    (0 until numBatches).map{ b =>
      val x = Array.fill(numPoints)(rand.nextGaussian())
      (0 until numKeys).map{ k =>
        val y = x.map(x => x * weights(0) + intercept + rand.nextGaussian() * noise)
        (k, y)
      } ++ Seq((numKeys + 1, x))
    }
  }

}
