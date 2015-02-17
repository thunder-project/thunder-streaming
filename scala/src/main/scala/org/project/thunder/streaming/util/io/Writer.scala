package org.project.thunder.streaming.util.io

import org.project.thunder.streaming.rdds.StreamingData

/**
 * The base class for all classes responsible for writing the contents of StreamingData objects (with key/value type
 * parameters K and V) out to disk
 */
trait Writer[K, V] {
  def write(data: List[(K, V)], fullFile: String)
}
