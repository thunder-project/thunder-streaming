package org.project.thunder_streaming.util

import scala.collection.mutable.HashMap

/*** Class for storing parameters related to an analysis in Thunder */

class ThunderParam {

  private val settings = new HashMap[String, String]()

  /** Set a parameter. */
  def set(key: String, value: String): ThunderParam = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    val current = settings.get(key).getOrElse("")
    if (current == "") {
      settings(key) = value
    } else {
      settings(key) = current + "\n" + value
    }
    this
  }

  /** Get any parameter, throw a NoSuchElementException if it's not set */
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  def toDebugString: String = {
    settings.toArray.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}

