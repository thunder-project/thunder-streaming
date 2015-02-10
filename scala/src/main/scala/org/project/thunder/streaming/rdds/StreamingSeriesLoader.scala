package org.project.thunder.streaming.rdds

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.streaming.StreamingContext

import org.project.thunder.streaming.util.io.Parser
import org.project.thunder.streaming.util.io.hadoop.FixedLengthBinaryInputFormat

class StreamingSeriesLoader(ssc: StreamingContext) {

  def fromText(dir: String, nKeys: Int): StreamingSeries = {
    val parser = new Parser(nKeys)
    val lines =ssc.textFileStream(dir)
    val dstream = lines.map(parser.getWithKey)
    new StreamingSeries(dstream)
   }

  def fromBinary(dir: String, nKeys: Int, valueType: String): StreamingSeries = {
    val parser = new Parser(nKeys, valueType)
    val lines = ssc.fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](dir)
    val dstream = lines.map{ case (k, v) => v.getBytes}.map(parser.getWithKey)
    new StreamingSeries(dstream)
  }

}
