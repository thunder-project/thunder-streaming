package org.project.thunder_streaming.util.launch

object Util {

  /**
   * Constructs an object of class T by calling its constructor with arguments args. The resulting object is properly
   * typed.
   *
   * Taken from http://stackoverflow.com/questions/1641104/instantiate-object-with-reflection-using-constructor-arguments
   * @param clazz The Class object parametrized by to be instantiated
   * @param args Arguments to the constructor
   * @tparam T The class to be instantiated
   * @return An instance of T
   */
  def instantiate[T](clazz: java.lang.Class[T])(args:AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(args:_*).asInstanceOf[T]
  }
}
