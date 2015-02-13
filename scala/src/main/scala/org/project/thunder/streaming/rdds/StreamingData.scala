package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream

trait StreamingData[K, V, +Self <: StreamingData[K, V, Self]] {

  val dstream: DStream[(K, V)]

  def apply(func: (K, V) => (K, V)): Self = {
    val output = dstream.map{p => func(p._1, p._2)}
    create(output)
  }

  def applyValues(func: V => V): Self = {
    val output = dstream.map{p => (p._1, func(p._2))}
    create(output)
  }

  def applyKeys(func: K => K): Self = {
    val output = dstream.map{p => (func(p._1), p._2)}
    create(output)
  }

  protected def create(dstream: DStream[(K, V)]): Self

}
