package org.yupana.core.cache

import org.yupana.core.cache.CacheFactory.CacheEngine.CacheEngine

abstract class CacheDescription(val name: String, val suffix: String, val engine: CacheEngine) {
  type Key
  def keyBoxing: BoxingTag[Key]

  type Value
  def valueBoxing: BoxingTag[Value]

  val fullName: String = s"${name}_$suffix"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: CacheDescription => this.name == that.name && this.suffix == that.suffix && this.engine == that.engine
      case _ => false
    }
  }

  override def hashCode(): Int = {
    ((37 * 17 + name.hashCode) * 17 + suffix.hashCode) * 17 + engine.hashCode()
  }

}

object CacheDescription {
  type Aux[K, V] = CacheDescription { type Key = K ; type Value = V}

  def apply[K, V](name: String, suffix: String, engine: CacheEngine)(implicit
    kTag: BoxingTag[K],
    vTag: BoxingTag[V]
  ): CacheDescription.Aux[K, V] = {
    new CacheDescription(name, suffix, engine) {
      override type Key = K
      override val keyBoxing: BoxingTag[K] = kTag

      override type Value = V
      override val valueBoxing: BoxingTag[V] = vTag
    }
  }
}
