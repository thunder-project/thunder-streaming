package thunder.streaming

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.util.StatCounter
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import thunder.util.{SaveStreaming, LoadStreaming, Save}
/**
 * To run: first build assembly jar:
 * `sbt assembly`
 *
 * Then pass assembled uber-jar to spark-submit:
 *
 *$SPARK_HOME/bin/spark-submit --master local --class thunder.streaming.StatefulStats \
 * ./target/scala-2.10/thunder-streaming-assembly-0.1.0_dev.jar \
 *  <master> <inputDirectory> <outputDirectory> [batch time]
 */


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
 * Default parameters for scopt option parser
 */
case class OptParams( master: String = null,
                      input: String = null,
                      output: String = null,
                      batchTime: Long = 5l)

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

    val parser = new scopt.OptionParser[OptParams]("StatefulStats") {
      head("StatefulStats", "0.1")
      arg[String]("<master>")
        .required()
        .text("URL for Spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<inputDirectory>")
        .required()
        .text("Path to directory of input data")
        .action((x, c) => c.copy(input = x))
      arg[String]("outputDirectory")
        .required()
        .text("Path to directory for output data")
        .action((x, c) => c.copy(output = x))
      arg[Long]("batch time")
        .optional()
        .text("Streaming batch time in s (whole numbers), default 5")
        .action((x, c) => c.copy(batchTime = x))
    }

    parser.parse(args, OptParams()) match {
      case Some(params) =>
        run(params)
      case None =>
        sys.exit(1)
    }
  }

  def run(params: OptParams) {
    val conf = new SparkConf().setMaster(params.master).setAppName("StatefulStats")

    if (!params.master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
          .setJars(List("target/scala-2.10/thunder-streaming_2.10-0.1.0_dev.jar"))
          .set("spark.executor.memory", "100G")
          .set("spark.default.parallelism", "100")
    }

    /** Create Streaming Context */
    val ssc = new StreamingContext(conf, Seconds(params.batchTime))
    ssc.checkpoint(System.getenv("CHECKPOINT"))

    /** Load streaming data */
    val data = LoadStreaming.fromBinary(ssc, params.input, format="short")

    /** Train stateful statistics models */
    val state = StatefulStats.trainStreaming(data)

    /** Print results (for testing) */
    state.mapValues(x => x.toString()).print()

    /** Get the mean */
    val result = state.mapValues(x => Array(x.mean)).filter{case (k, v) => k < 512 * 512 * 4}

    /** Collect output */
    SaveStreaming.asBinaryWithKeys(result, params.output, Seq("mean"))

    ssc.start()
    ssc.awaitTermination()
  }

}
