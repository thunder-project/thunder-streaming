
package org.project.thunder.streaming.regression

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import org.project.thunder.streaming.util.LoadParam
import org.project.thunder.streaming.util.counters.{StatCounterMixed, StatUpdater}
import org.project.thunder.streaming.util.io.Keys


/**
 * Stateful binned statistics
 */
class StatefulBinnedStats (
  var featureKeys: Array[Int],
  var nfeatures: Int)
  extends Serializable with Logging
{

  def this() = this(Array(0), 1)

  /** Set which indices that correspond to features. */
  def setFeatureKeys(featureKeys: Array[Int]): StatefulBinnedStats = {
    this.featureKeys = featureKeys
    this
  }

  /** Set the values associated with the to features. */
  def setFeatureCount(nfeatures: Int): StatefulBinnedStats = {
    this.nfeatures = nfeatures
    this
  }

  def runStreaming(data: DStream[(Int, Array[Double])]): DStream[(Int, StatCounterMixed)] = {

    var features = Array[Double]()

    // extract the bin labels
    data.filter{case (k, _) => featureKeys.contains(k)}.foreachRDD{rdd =>
      val batchFeatures = rdd.values.collect().flatten
      features = batchFeatures.size match {
        case 0 => Array[Double]()
        case _ => batchFeatures
      }
      println(features.mkString(" "))
    }

    // update the stats for each key
    data.filter{case (k, _) => !featureKeys.contains(k)}.updateStateByKey{
      (x, y) => StatUpdater.mixed(x, y, features, nfeatures)
    }

  }

}

/**
 * Top-level methods for calling Stateful Binned Stats.
 */
object StatefulBinnedStats {

  /**
   * Compute running statistics on keyed data points in bins.
   * For each key, statistics are computed within each of several bins
   * specified by auxiliary data passed as a special key.
   * We assume that in each batch of streaming data we receive
   * an array of doubles for each data key, and an array of integer indices
   * for the bin key. We use a StatCounterArray on
   * each key to update the statistics within each bin.
   *
   * @param input DStream of (Int, Array[Double]) keyed data point
   * @return DStream of (Int, BinnedStats) with statistics
   */
  def trainStreaming(input: DStream[(Int, Array[Double])],
                     featureKeys: Array[Int],
                     featureCount: Int): DStream[(Int, StatCounterMixed)] =
  {
    new StatefulBinnedStats().setFeatureKeys(featureKeys).setFeatureCount(featureCount).runStreaming(input)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: StatefulBinnedStats <master> <directory> <batchTime> <outputDirectory> <paramFile>")
      System.exit(1)
    }

    val (master, directory, batchTime, outputDirectory, paramFile) = (
      args(0), args(1), args(2).toLong, args(3), args(4))

    val conf = new SparkConf().setMaster(master).setAppName("StatefulStats")

    if (!master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
        .setJars(List("target/scala-2.10/thunder_2.10-0.1.0.jar"))
        .set("spark.executor.memory", "100G")
        .set("spark.default.parallelism", "100")
    }

    /** Create Streaming Context */
    val ssc = new StreamingContext(conf, Seconds(batchTime))
    ssc.checkpoint(System.getenv("CHECKPOINT"))

    /** Load analysis parameters */
    val params = LoadParam.fromText(paramFile)

    /** Get feature keys with linear indexing */
    val binKeys = Keys.subToInd(params.getBinKeys, params.getDims)

    /** Get values for the features */
    val binValues = params.getBinValues

    /** Get names for the bins */
    val binName = params.getBinName

    /** Load streaming data */
    //val data = LoadStreaming.fromBinaryWithKeys(ssc, directory, params.getDims.size, params.getDims, format="short")

    /** Train the models */
    //val state = StatefulBinnedStats.trainStreaming(data, binKeys, binValues(0).length)

    /** Print results (for testing) */
    //val result = state.mapValues(x => Array(x.r2, x.weightedMean(binValues(0))))

    /** Collect output */
    //SaveStreaming.asBinaryWithKeys(result, outputDirectory, Seq("r2-" + binName(0), "tuning-" + binName(0)))

    //ssc.start()
    //ssc.awaitTermination()
  }

}
