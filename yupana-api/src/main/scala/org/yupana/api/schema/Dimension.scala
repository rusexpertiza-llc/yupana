package org.yupana.api.schema

case class Dimension(name: String, hashFunction: Option[String => Int] = None) {

  def hash(v: String): Int  = _hash(v)

  private val _hash: String => Int = hashFunction.getOrElse(zeroHash)

  private def zeroHash(s: String): Int = 0

  override def hashCode(): Int = name.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case Dimension(n, _) => name == n
    case _ => false
  }
}
