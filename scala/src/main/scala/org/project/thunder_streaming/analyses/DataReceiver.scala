package org.project.thunder_streaming.analyses

import org.project.thunder_streaming.util.ThunderStreamingContext
import org.zeromq.ZMQ

/**
 * Receives updates from an Analysis' corresponding Python Analysis (in the manager) using a simple TCP-based
 *
 */
abstract class DataReceiver(listener: Updatable, params: AnalysisParams) {
  def receive(): Unit
}

object DataReceiver {
  def getDataReceiver(tssc: ThunderStreamingContext, listener: Updatable, params: AnalysisParams = new AnalysisParams(Map[String, List[String]]())): DataReceiver = {
    new ZeroMQReceiver(tssc, listener, params)
  }

  final val FORWARDER_ADDR = "dr_forwarder_addr"
  final val TAGS = "dr_subscription"
}


class ZeroMQReceiver(tssc: ThunderStreamingContext, listener: Updatable, params: AnalysisParams) extends DataReceiver(listener, params) {

  val sub_sock = tssc.context.socket(ZMQ.SUB)
  sub_sock.connect(params.getSingleParam(DataReceiver.FORWARDER_ADDR))
  val subscribers = params.getParam(DataReceiver.TAGS)
  // Subscribe to each of the subscribers
  println("%s subscribing to %s".format(this.toString, subscribers))
  subscribers.map{ s =>sub_sock.subscribe(s.getBytes()) }

  override def receive(): Unit = {
    val tag = new String(sub_sock.recv(0))
    val msg = new String(sub_sock.recv(0))
    listener.handleUpdate((tag, msg))
  }
}

