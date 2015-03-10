package org.project.thunder.streaming.analyses

/**
 * Receives updates from an Analysis' corresponding Python Analysis (in the manager) using a simple TCP-based
 *
 */

abstract class DataReceiver(listener: Updatable, params: Map[String, String]) {
  def connect(): Unit
  def receive(): Unit
}

object DataReceiver {
  def getDataReceiver(listener: Updatable, params: Map[String, String] = Map[String, String]()): DataReceiver = {
    new TCPDataReceiver(listener, params)
  }
}

class ZeroMQReceiver(listener: Updatable, params: Map[String, String]) extends DataReceiver(listener, params) {
  override def connect(): Unit = ???

  override def receive(): Unit = ???
}

class TCPDataReceiver(listener: Updatable, params: Map[String, String]) extends DataReceiver(listener, params) {

  def connect(): Unit = {
    val host = params.getOrElse("host", "localhost")
    val port = params.getOrElse("port", "0").toInt
    /*
    for {
      conn <-
    }
    */
  }

  def receive(): Unit = {

  }

}
