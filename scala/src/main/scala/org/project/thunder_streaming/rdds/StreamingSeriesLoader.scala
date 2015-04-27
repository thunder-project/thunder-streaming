package org.project.thunder_streaming.rdds

import org.apache.hadoop.io.{BytesWritable, LongWritable}

import org.apache.spark.streaming.StreamingContext
import org.project.thunder_streaming.util.io.Parser
import org.project.thunder_streaming.util.io.hadoop.FixedLengthBinaryInputFormat

class StreamingSeriesLoader(ssc: StreamingContext) {

  /** Load streaming series data from text */
  def fromText(dir: String): StreamingSeries = {
    val parser = new Parser()
    val lines =ssc.textFileStream(dir)
    val dstream = lines.map(parser.getWithKey)
    new StreamingSeries(dstream)
   }

  /** Load streaming series data from binary */
  def fromBinary(dir: String, valueType: String): StreamingSeries = {
    val parser = new Parser(valueType)
    val lines = ssc.fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](dir)
    val dstream = lines
      .map{ case (k, v) => (k.get().toInt, v.getBytes)}
      .map{ case (k, v) => (k, parser.get(v))}
    new StreamingSeries(dstream)
  }

}
