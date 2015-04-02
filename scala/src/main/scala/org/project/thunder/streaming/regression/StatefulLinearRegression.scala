package org.project.thunder.streaming.regression

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.Logging
import org.project.thunder.streaming.rdds.StreamingSeries

import scala.math.sqrt
import cern.colt.matrix.{DoubleFactory1D, DoubleFactory2D, DoubleMatrix1D, DoubleMatrix2D}
import cern.jet.math.Functions.{plus, minus, bindArg2, pow}
import cern.colt.matrix.linalg.Algebra.DEFAULT.{inverse, mult, transpose}

import org.project.thunder.streaming.util.io.Keys

/** Class for representing parameters and sufficient statistics for a running linear regression model */
class FittedModel(
   var count: Double,
   var mean: Double,
   var sumOfSquaresTotal: Double,
   var sumOfSquaresError: Double,
   var XX: DoubleMatrix2D,
   var Xy: DoubleMatrix1D,
   var beta: DoubleMatrix1D) extends Serializable {

  def variance = sumOfSquaresTotal / (count - 1)

  def std = sqrt(variance)

  def r2 = 1 - sumOfSquaresError / sumOfSquaresTotal

  def intercept = beta.toArray()(0)

  def weights = beta.toArray.drop(1)

}

/**
* Stateful linear regression on streaming data
*
* The underlying model is that every batch of streaming
* data contains a set of records with unique keys,
* one is the feature, and the rest can be predicted
* by the feature. We estimate the sufficient statistics
* of the features, and each of the data points,
* to computing a running estimate of the linear regression
* model for each key. Returns a state stream of fitted models.
*
* Features and labels from different batches
* can have different lengths.
*/
class StatefulLinearRegression (
  var featureKeys: Array[Int],
  var selectedKeys: Array[Int])
  extends Serializable with Logging
{

  def this() = this(Array(0), Array(0))

  /** Set which indices that correspond to features. Default: Array(0). */
  def setFeatureKeys(featureKeys: Array[Int]): StatefulLinearRegression = {
    this.featureKeys = featureKeys
    this
  }

  /** Set which indices to use as features in regression. Default: Array(0). */
  def setSelectedKeys(selectedKeys: Array[Int]): StatefulLinearRegression = {
    this.selectedKeys = selectedKeys
    this
  }

  val runningLinearRegression = (
    input: Seq[Array[Double]],
    state: Option[FittedModel],
    features: Array[Double]) => {

    println("In runningLinearRegression, features: %s, input: %s".format(features.mkString(","),
      input.map(_.mkString(","))))

    val y = input.foldLeft(Array[Double]()) { (acc, i) => acc ++ i}
    val currentCount = y.size
    val n = features.size

    val updatedState = state.getOrElse(new FittedModel(0.0, 0.0, 0.0, 0.0, DoubleFactory2D.dense.make(2, 2),
      DoubleFactory1D.dense.make(2), DoubleFactory1D.dense.make(2)))

    if ((currentCount != 0) & (n != 0)) {

      // append column of 1s
      val X = DoubleFactory2D.dense.make(n, 2)
      for (i <- 0 until n) {
        X.set(i, 0, 1)
      }
      for (i <- 0 until n ; j <- 1 until 2) {
        X.set(i, j, features(i))
      }

      // create matrix version of y
      val ymat = DoubleFactory1D.dense.make(currentCount)
      for (i <- 0 until currentCount) {
        ymat.set(i, y(i))
      }

      // store values from previous iteration (needed for update equations)
      val oldCount = updatedState.count
      val oldMean = updatedState.mean
      val oldXy = updatedState.Xy.copy
      val oldXX = updatedState.XX.copy
      val oldBeta = updatedState.beta

      // compute current estimates of all statistics
      val currentMean = y.foldLeft(0.0)(_+_) / currentCount
      val currentSumOfSquaresTotal = y.map(x => pow(x - currentMean, 2)).foldLeft(0.0)(_+_)
      val currentXy = mult(transpose(X), ymat)
      val currentXX = mult(transpose(X), X)

      // compute new values for X*y (the sufficient statistic) and new beta (needed for update equations)
      val newXX = oldXX.copy.assign(currentXX, plus)
      val newXy = updatedState.Xy.copy.assign(currentXy, plus)
      val newBeta = mult(inverse(newXX), newXy)

      // compute terms for update equations
      val delta = currentMean - oldMean
      val term1 = ymat.copy.assign(mult(X, newBeta), minus).assign(bindArg2(pow, 2)).zSum
      val term2 = mult(mult(oldXX, newBeta), newBeta)
      val term3 = mult(mult(oldXX, oldBeta), oldBeta)
      val term4 = 2 * mult(oldBeta.copy.assign(newBeta, minus), oldXy)

      // update the all statistics of the fitted model
      updatedState.count += currentCount
      updatedState.mean += (delta * currentCount / (oldCount + currentCount))
      updatedState.Xy = newXy
      updatedState.XX = newXX
      updatedState.beta = newBeta
      updatedState.sumOfSquaresTotal += currentSumOfSquaresTotal +
        delta * delta * (oldCount * currentCount) / (oldCount + currentCount)
      updatedState.sumOfSquaresError += term1 + term2 - term3 + term4
    }

    Some(updatedState)
  }


  def fit(data: StreamingSeries): DStream[(Int, FittedModel)] = {

    var features = Array[Double]()

    data.dstream.filter{case (k, _) => selectedKeys.contains(k)}.foreachRDD{rdd =>
        val batchFeatures = rdd.values.collect().flatten
        features = batchFeatures.size match {
          case 0 => Array[Double]()
          case _ => batchFeatures
        }
    }

    data.dstream.filter{case (k, _) => !featureKeys.contains(k)}.updateStateByKey{
      (values, state) => runningLinearRegression(values, state, features)}
    }

}

/**
* Top-level methods for calling Stateful Linear Regression.
*/
object StatefulLinearRegression {

  /**
   * Train a Stateful Linear Regression model.
   * We assume that in each batch of streaming data we receive
   * one or more features and several vectors of labels, each
   * with a unique key, and a subset of keys indicate the features.
   * We fit separate linear models that relate the common features
   * to the labels associated with each key.
   *
   * @param input DStream of (Int, Array[Double]) keyed data point
   * @param featureKeys Array of keys associated with features
   * @return StreamingSeries with parameters of fitted regression models
   */
  def run(
    input: StreamingSeries,
    featureKeys: Array[Int],
    selectedKeys: Array[Int]): StreamingSeries =
  {

    val output = new StatefulLinearRegression()
      .setFeatureKeys(featureKeys)
      .setSelectedKeys(selectedKeys)
      .fit(input)
      .mapValues(x => Array(x.r2) ++ x.beta.toArray)

    new StreamingSeries(output)

  }

}
