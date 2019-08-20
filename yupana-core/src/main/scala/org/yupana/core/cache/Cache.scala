package org.yupana.core.cache

trait Cache[K, V] {
  def get(key: K): Option[V]
  def getNullable(key: K): V
  def put(key: K, value: V): Unit
  def remove(key: K): Boolean
  def removeAll(): Unit
  def getAll(keys: Set[K]): Map[K, V]
  def putAll(batch: Map[K, V]): Unit
  def contains(key: K): Boolean

  def caching(key: K)(eval: => V): V = {
    val value = getNullable(key)
    if (value == null) {
      val v = eval
      put(key, v)
      v
    } else {
      value
    }
  }

  def allCaching(keys: Set[K])(eval: Set[K] => Map[K, V]): Map[K, V] = {
    val cached = getAll(keys)
    val missing = keys -- cached.keys
    val evaluated = if (missing.nonEmpty) {
      val e = eval(missing)
      putAll(e)
      e
    } else {
      Seq.empty
    }

    cached ++ evaluated
  }
}


