package thunder.streaming

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.util.StatCounter
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import thunder.util.{SaveStreaming, LoadStreaming, Save}

/**
 * Stateful statistics
 */
class StatefulStats ()
  extends Serializable with Logging
{

  /** State updating function that updates the statistics for each key. */
  val runningStats = (values: Seq[Array[Double]], state: Option[StatCounter]) => {
    val updatedState = state.getOrElse(new StatCounter())
    val vec = values.flatten
    Some(updatedState.merge(vec))
  }

  def runStreaming(data: DStream[(Int, Array[Double])]): DStream[(Int, StatCounter)] = {
      data.updateStateByKey{runningStats}
  }

}

/**
 * Top-level methods for calling Stateful Stats.
 */
object StatefulStats {

  /**
   * Compute running statistics.
   * We assume that in each batch of streaming data we receive
   * an array of doubles for each key, and use a StatCounter on
   * each key to update the statistics.
   *
   * @param input DStream of (Int, Array[Double]) keyed data point
   * @return DStream of (Int, StatCounter) with statistics
   */
  def trainStreaming(input: DStream[(Int, Array[Double])]): DStream[(Int, StatCounter)] =
  {
    new StatefulStats().runStreaming(input)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: StatefulStats <master> <inputDirectory> <outputDirectory> <batchTime>")
      System.exit(1)
    }

    val (master, directory, outputDirectory, batchTime) = (
      args(0), args(1), args(2), args(3).toLong)

    val conf = new SparkConf().setMaster(master).setAppName("StatefulStats")

    if (!master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
          .setJars(List("target/scala-2.10/thunder-streaming_2.10-0.1.0_dev.jar"))
          .set("spark.executor.memory", "100G")
    }

    /** Create Streaming Context */
    val ssc = new StreamingContext(conf, Seconds(batchTime))
    ssc.checkpoint(System.getenv("CHECKPOINT"))

    /** Load streaming data */
    val data = LoadStreaming.fromBinary(ssc, directory, format="short")

    /** Train stateful statistics models */
    val state = StatefulStats.trainStreaming(data)

    /** Print results (for testing) */
    state.mapValues(x => x.toString()).print()

    /** Get the mean */
    val result = state.mapValues(x => Array(x.mean)).filter{case (k, v) => k < 512 * 512 * 4}

    /** Collect output */
    SaveStreaming.asBinaryWithKeys(result, outputDirectory, Seq("mean"))

    ssc.start()
    ssc.awaitTermination()
  }

}
