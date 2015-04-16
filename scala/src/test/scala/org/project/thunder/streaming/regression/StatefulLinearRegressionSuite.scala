package org.project.thunder.streaming.regression

import org.apache.spark.streaming.TestSuiteBase

import org.apache.spark.streaming.dstream.DStream
import org.project.thunder.streaming.rdds.StreamingSeries
import org.scalatest.{FunSuite, BeforeAndAfterEach}

import scala.util.Random
import math.abs

import org.project.thunder.streaming.util.TestUtils._

abstract class LinearRegressionSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 5000

  var numBatches = 20
  var numPoints = 10
  var numKeys = 5
  var intercept = 1.0 // intercept for linear model
  var noise = 1.0 // noise level
  var rand = new Random(42)
  var weights: Array[Double] = _
  var output: Seq[Seq[(Int, FittedModel)]] = _

  def getModel(featureKeys: Array[Int], selectedKeys: Array[Int]): StatefulLinearRegression = {
    new StatefulLinearRegression().setFeatureKeys(featureKeys).setSelectedKeys(selectedKeys)
  }

  def getDefaultModel(): StatefulLinearRegression = {
    val featureKeys =  (1 to weights.length).map{x => numKeys + x}.toArray[Int]
    val selectedKeys = (1 to weights.length).map{x => numKeys + x}.toArray[Int]
    getModel(featureKeys, selectedKeys)
  }

  def setup(model: StatefulLinearRegression = getDefaultModel()) {
    val input = RegressionStreamGenerator(
      intercept, weights, rand, numBatches, numPoints, numKeys, noise)

    val ssc = setupStreams(input, (inputDStream: DStream[(Int, Array[Double])]) => {
      val series = new StreamingSeries(inputDStream)
      model.fit(series)
    })

    output = runStreams(ssc, numBatches, numBatches)
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
        val x = Array.fill(numPoints)(weights.map(w => rand.nextGaussian()))
        (0 until numKeys).map{ k =>
          val y = x.map(x => x.zipWithIndex.foldLeft(0.0){ case (sum, (i, idx)) => sum + (i * weights(idx)) }
                      + intercept + rand.nextGaussian() * noise)
          (k, y)
        } ++ weights.zipWithIndex.map{ case (w, idx) => (numKeys + idx + 1, x.map{ xi => xi(idx) })}
      }
    }

  test("weight accuracy") {
    setup()
    output.last.foreach{ case (k, v) =>
      assertEqual(v.weights, weights, 0.1)
    }
  }

  test("convergence") {
    // is the error in the betas on the first batch higher than on the final batch

    def weightDiff(computedWeights: Seq[Array[Double]]): Double = {
      val diffs = computedWeights.map(arr => arr.zip(weights).map{ case (x,y) => abs(x - y) }.reduce(_ + _ ))
      diffs.sum
    }

    setup()

    val firstDiff = weightDiff(output.take(1)(0).map{ case (k, v) => v.weights })
    val lastDiff = weightDiff(output.last.map{ case (k, v) => v.weights })
    assert(firstDiff > lastDiff)
  }

  test("r2") {
    // is it perfect when you have no noise
    // is it lower when you have more noise

    def getR2s(output: Seq[Seq[(Int, FittedModel)]]): Seq[Double] = {
      output.last.map{ case (k, v) => v.r2 }
    }

    def setupWithNoise(n: Double): Unit = {
      noise = n
      setup()
    }

    setupWithNoise(0.0)
    val perfectR2 = getR2s(output).sum

    setupWithNoise(0.5)
    val worseR2 = getR2s(output).sum

    setupWithNoise(1.5)
    val worstR2 = getR2s(output).sum

    assertEqual(perfectR2, output.last.length, 0.0001)
    assert(worseR2 > worstR2)
  }

  test("key selection") {
    setup()

    // are the right features used?
    // is it ok to have different sets of featureKeys and selectedKeys

  }

  test("normalized betas") {
    setup()
    // output.last.map()
  }
}


class MultiFeatureRegressionSuite extends LinearRegressionSuite {
  weights = Array(1.0, 2.0, 3.0)
}


class SingleFeatureRegressionSuite extends LinearRegressionSuite {
  weights = Array(1.0)
}
