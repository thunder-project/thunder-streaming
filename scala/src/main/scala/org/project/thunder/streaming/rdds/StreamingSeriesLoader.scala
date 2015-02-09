package org.project.thunder.streaming.rdds

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.streaming.StreamingContext

import org.project.thunder.streaming.util.io.Parser
import org.project.thunder.streaming.util.io.hadoop.FixedLengthBinaryInputFormat

class StreamingSeriesLoader(ssc: StreamingContext) {

  def fromText(dir: String, nkeys: Int): StreamingSeries = {
    val parser = new Parser(nkeys)
    val lines =ssc.textFileStream(dir)
    val dstream = lines.map(parser.getWithKeys)
    new StreamingSeries(dstream)
   }

  def fromBinary(
      dir: String,
      nkeys: Int,
      nvalues: Int,
      keyType: String,
      valueType: String = "short"): StreamingSeries = {
    val parser = new Parser(nkeys)
    val lines = ssc.fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](dir)
    val dstream = lines.map{ case (k, v) => v.getBytes}.map(parser.getWithKeys)
    new StreamingSeries(dstream)
  }

}
