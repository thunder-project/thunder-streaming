package org.project.thunder.streaming.rdds

import org.apache.spark.SparkContext._

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

abstract class StreamingData[V: ClassTag, +Self <: StreamingData[V, Self]] extends Serializable {

  val dstream: DStream[(Int, V)]

  /** Apply a function to keys and values, and reconstruct the class */
  def apply(func: (Int, V) => (Int, V)): Self = {
    val output = dstream.map{p => func(p._1, p._2)}
    create(output)
  }

  /** Apply a function to the values, and reconstruct the class */
  def applyValues(func: V => V): Self = {
    val output = dstream.map{p => (p._1, func(p._2))}
    create(output)
  }

  /** Apply a function to the keys, and reconstruct the class */
  def applyKeys(func: Int => Int): Self = {
    val output = dstream.map{p => (func(p._1), p._2)}
    create(output)
  }

  /** Output the values by collecting and passing to one or more functions */
  def output(func: List[(List[V], Time) => Unit]): Unit = {
    dstream.foreachRDD { (rdd, time) =>
      val out = rdd.collect().map{case (k, v) => v}.toList
      func.foreach(f => f(out, time))
    }
  }

  /** Output the values and keys by collecting and passing to one or more functions */
  def outputWithKeys(func: List[(List[(Int, V)], Time) => Unit]): Unit = {
    dstream.foreachRDD { (rdd, time) =>
      val out = rdd.sortByKey().collect().toList
      func.foreach(f => f(out, time))
    }
  }

  /** Does a standard filter operation on the underlying DStream and returns a new StreamingData object **/
  def filter(func: ((Int, V)) => Boolean): Self = {
    val filteredStream = dstream.filter(func)
    create(filteredStream)
  }

  /** Filters the underlying DStream based on a function applied to its values and returns a new StreamingData object **/
  def filterOnValues(func: V => Boolean): Self = {
    val filteredStream = dstream.filter{case (k, v) => func(v)}
    create(filteredStream)
  }

  /** Filters the underlying DStream based on a function applied to its keys and returns a new StreamingData object **/
  def filterOnKeys(func: Int => Boolean): Self = {
    val filteredStream = dstream.filter{case (k, v) => func(k)}
    create(filteredStream)
  }

  /** Print the records (useful for debugging) **/
  def print()

  protected def create(dstream: DStream[(Int, V)]): Self
}