package org.project.thunder.streaming.util.io

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.streaming.dstream.DStream
import java.util.Calendar

/**
 * Generic writer class for writing the contents of an RDD or DStream to disk.
 * Separate methods for handing RDDs with or without keys.
 *
 * This class should be extended by defining a write method specific
 * to a particular output type (see e.g. TextWriter, BinaryWriter).
 *
 */
abstract class Writer {

  def write(rdd: RDD[Double], fullFile: String)

  def withoutKeys(data: DStream[(Array[Double])], directory: String, fileName: Seq[String]) {
    data.foreachRDD{rdd =>
      if (rdd.count() > 0) {
        val n = rdd.first().size
        val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
        for (i <- 0 until n) {
          write(rdd.map(x => x(i)), directory ++ File.separator ++ fileName(i) ++ "-" ++ dateString)
        }
      }
    }
  }

  def withKeys(data: DStream[(List[Int], Array[Double])], directory: String, fileName: Seq[String]) {
    data.foreachRDD{rdd =>
      if (rdd.count() > 0) {
        val sorted = rdd.sortByKey().values
        val n = sorted.first().size
        val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
        for (i <- 0 until n) {
          write(sorted.map(x => x(i)), directory ++ File.separator ++ fileName(i) ++ "-" ++ dateString)
        }
      }
    }
  }

}