package org.project.thunder.streaming.rdds

import org.apache.spark.streaming.dstream.DStream

abstract class StreamingData[K, V, +Self <: StreamingData[K, V, Self]]
  (val dstream: DStream[(K, V)]) {

  def apply(func: (K, V) => (K, V)): Self = {
    val output = dstream.map{p => func(p._1, p._2)}
    makeNew(output)
  }

  def applyValues(func: V => V): Self = {
    val output = dstream.map{p => (p._1, func(p._2))}
    makeNew(output)
  }

  def applyKeys(func: K => K): Self = {
    val output = dstream.map{p => (func(p._1), p._2)}
    makeNew(output)
  }

  protected def makeNew(dstream: DStream[(K, V)]): Self

}
