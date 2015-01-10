package thunder.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import thunder.util.LoadStreaming

object ExampleJoin {

  def main(args: Array[String]) {

    val master = args(0)

    val file1 = args(1)

    val file2 = args(2)

    val batchTime = args(3).toLong

    val conf = new SparkConf().setMaster(master).setAppName("ExampleJoin")

    if (!master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
        .setJars(List("target/scala-2.10/thunder_2.10-0.1.0.jar"))
        .set("spark.executor.memory", "100G")
    }

    val ssc = new StreamingContext(conf, Seconds(batchTime))
    ssc.checkpoint("~/checkpoint")

    val runningStats = (values: Seq[Array[Double]], state: Option[Array[Double]]) => {
      val updatedState = state.getOrElse(Array[Double](0.0))
      val vec = values.flatten.toArray
      Some(Array.concat(vec, updatedState))
    }

    val data1 = LoadStreaming.fromTextWithKeys(ssc, file1)

    //val data2 = LoadStreaming.fromTextWithKeys(ssc, file2).transform((rdd, time) => rdd.map(v => (time, v)))

    //val data3 = data1.union(data2)

    //data3.map{case (k,v) => k.toString() + v.mkString(" ")}.print()

    //data3.foreachRDD(rdd => println(rdd.count()))

    val foo = data1.updateStateByKey(runningStats)

    foo.map(p => (p._1, p._2.mkString(" "))).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
