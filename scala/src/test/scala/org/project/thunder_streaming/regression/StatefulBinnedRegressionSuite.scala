package org.project.thunder_streaming.regression

import org.apache.spark.streaming.dstream.DStream
import org.project.thunder_streaming.rdds.StreamingSeries
import org.project.thunder_streaming.util.TestUtils
import org.project.thunder_streaming.util.counters.StatCounterMixed

import scala.util.Random

import org.apache.spark.streaming.TestSuiteBase
import org.scalatest.FunSuite

import TestUtils._
import math.abs

class StatefulBinnedRegressionSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 5000

  var numBatches = 50
  var numPoints = 30
  var numKeys = 10
  var numBins = 5
  var binSize = 10
  var firstEdge = 0
  var noise = 2.0
  var rand = new Random(42)

  // The BinnedRegressionStreamGenerator will produce a stream of values with high mean values in this bin
  var tunedBin = 3
  val lowMean = 1
  val highMean = 10

  var output: Seq[Seq[(Int, StatCounterMixed)]] = _

  def leftEdges: Array[Double] = Range(0, numBins + 1).map(idx => firstEdge.toDouble + (idx * binSize)).toArray[Double]

  def binCenters(edges: Array[Double]) = edges.map(edge => edge + (binSize / 2.0))

  def getModel(featureKey: Int, leftEdges: Array[Double]): StatefulBinnedRegression = {
    new StatefulBinnedRegression().setFeatureKey(featureKey).setLeftEdges(leftEdges)
  }

  def getDefaultModel(): StatefulBinnedRegression = {
    new StatefulBinnedRegression().setFeatureKey(numKeys).setLeftEdges(leftEdges)
  }

  def setup(model: StatefulBinnedRegression = getDefaultModel()): Unit = {
    val input = BinnedRegressionStreamGenerator(
      leftEdges, tunedBin, lowMean, highMean, rand, numBatches, numPoints, noise
    )

    val ssc = setupStreams(input, (inputDStream: DStream[(Int, Array[Double])]) => {
      val series = new StreamingSeries(inputDStream)
      model.fit(series)
    })

    output = runStreams(ssc, numBatches, numBatches)
  }

  def BinnedRegressionStreamGenerator(
    leftEdges: Array[Double],
    tunedBins: Int,
    lowMean: Double,
    highMean: Double,
    rand: Random,
    numBatches: Int,
    numPoints: Int,
    noise: Double): Seq[Seq[(Int, Array[Double])]] = {
     val centers = binCenters(leftEdges)
     // Assume uniform-sized bins
     val halfBinSize = centers(0) - leftEdges(0)
     (0 until numBatches).map{ b =>
       val binAndX: Array[(Int, Double)] = Range(0, numPoints).map(x => {
         val curBin = rand.nextInt(centers.size)
         val curCenter = centers(curBin)
         (curBin, curCenter + (rand.nextGaussian() * halfBinSize))
       }).toArray[(Int, Double)]
       (0 until numKeys).map { k =>
         val y = binAndX.map { case (bin, x) =>
           val curNoise = noise * rand.nextGaussian()
           if (tunedBin == bin) highMean + curNoise else lowMean + curNoise
         }
         (k, y)
       } :+ (numKeys, binAndX.map{ case (bin, x) => x })
    }
  }

  test("weight accuracy") {
    val featureKey = numKeys
    setup(getModel(featureKey, leftEdges))
    val centers = binCenters(leftEdges)

    output.last.filter { case (k, v) => k != featureKey }.foreach {
      case (k, v) => assertEqual(v.weightedMean(centers), centers(tunedBin), 0.1)
    }
  }

  test("convergence") {
    val featureKey = numKeys
    setup(getModel(featureKey, leftEdges))
    val centers = binCenters(leftEdges)

    val target = centers(tunedBin)
    def weightedMeanDiff(batch: Seq[(Int, StatCounterMixed)]): Double = {
      batch.map{ case (idx, sc) => abs(sc.weightedMean(centers) - target) }.reduce(_+_)
    }

    val firstDiff = weightedMeanDiff(output.take(1)(0))
    val lastDiff = weightedMeanDiff(output.last)

    assert(firstDiff > lastDiff)
  }

  test("r2") {
    val featureKey = numKeys

    def getR2s(output: Seq[Seq[(Int, StatCounterMixed)]]): Seq[Double] = {
      output.last.filter{ case (k,v) => k != featureKey }.map{ case (k, v) => v.r2 }
    }

    def setupWithNoise(n: Double): Unit = {
      noise = n
      setup(getModel(featureKey, leftEdges))
    }

    setupWithNoise(0.0)
    val perfectR2 = getR2s(output).sum

    setupWithNoise(2)
    val worseR2 = getR2s(output).sum

    setupWithNoise(4)
    val worstR2 = getR2s(output).sum

    assert(perfectR2 > worseR2)
    assert(worseR2 > worstR2)
  }
}
