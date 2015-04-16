package org.project.thunder_streaming.util.io

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors


/**
 * Class for loading lines of streaming data files
 * supporting a variety of formats
 *
 * @param format Byte encoding
 */
case class Parser(format: String = "short") {

  /**
   * Convert an Array[Byte] to Array[Double]
   * using a Java ByteBuffer, where "format"
   * specifies the byte encoding scheme (and which
   * ByteBuffer subclass to use)
   */
  def convertBytes(v: Array[Byte]): Array[Double] = {

    format match {
      case "short" => {
        val buffer = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        val intArray = new Array[Int](buffer.remaining())
        var t = 0
        while (buffer.remaining() > 0) {
          intArray(t) = buffer.get()
          t += 1
        }
        intArray.map(_.toDouble)
      }
      case "int" => {
        val buffer = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
        val intArray = new Array[Int](buffer.remaining())
        var t = 0
        while (buffer.remaining() > 0) {
          intArray(t) = buffer.get()
          t += 1
        }
        intArray.map(_.toDouble)
      }
      case "double" => {
        val buffer = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer()
        val DoubleArray = new Array[Double](buffer.remaining())
        var t = 0
        while (buffer.remaining() > 0) {
          DoubleArray(t) = buffer.get()
          t += 1
        }
        DoubleArray
      }
    }
  }

  /** Parse all entries as values */
  def get(line: String): Array[Double] = {
    val parts = line.split(' ')
    val value = parts.map(_.toDouble)
    value
  }

  /** Parse all Ints as values */
  def get(line: Array[Int]): Array[Double] = {
    val value = line.map(_.toDouble)
    value
  }

  /** Parse all bytes into values */
  def get(line: Array[Byte]): Array[Double] = {
    val value = convertBytes(line)
    value
  }

  /** Parse first records as keys, then values */
  def getWithKey(line: String): (Int, Array[Double]) = {
    val parts = line.split(' ')
    val key = parts(0).toInt
    val value = parts.slice(1, parts.length).map(_.toDouble)
    (key, value)
  }

  /** Parse first Ints as keys, then values */
  def getWithKey(line: Array[Int]): (Int, Array[Double]) = {
    val key = line(0)
    val value = line.slice(1, line.length).map(_.toDouble)
    (key, value)
  }

  /** Parse first Bytes as keys, then values */
  def getWithKey(line: Array[Byte]): (Int, Array[Double]) = {
    val parts = convertBytes(line)
    val key = parts(0).toInt
    val value = parts.slice(1, line.length)
    (key, value)
  }

}