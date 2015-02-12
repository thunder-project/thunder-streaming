package org.project.thunder.streaming.util.launch

import scala.xml.{Node, NodeSeq}

/**
 * Created by Andrew on 2/11/15.
 */
object Util {

  /**
   * Given a node, finds the first node with the given element name, ensure that it doesn't have any children, and
   * extracts its text
   *
   * @param node The parent node (NodeSeq for compatibility with Scala's XML API)
   * @param nodeName The node whose text is being extracted
   * @return The extracted text, if found
   */
  def getFirstNodeText(node: NodeSeq, nodeName: String): Option[String] = {
    val matchingNodes = node \ nodeName
    if (matchingNodes.length > 0) {
      val firstNode = matchingNodes(0)
      if firstNode.child.length == 1
    }
  }

  /**
   * Given a node, checks whether that node has any child elements, or is a leaf (surprisingly couldn't find this in
   * the XML API
   *
   * TODO is there a more idiomatic Scala way of doing this?
   *
   * @param node The parent node (NodeSeq for compatibility with Scala's XML API)
   * @return Boolean saying whether the node is a leaf
   */
  def isLeafNode(node: NodeSeq): Boolean = {

  }

}
