package org.yupana.core.cache

import scala.reflect.ClassTag

trait BoxingTag[T] {
  type R <: AnyRef
  def clazz: Class[R]
}

object BoxingTag {
  def apply[T](implicit b: BoxingTag[T]): BoxingTag[T] = b

  implicit def valBoxing[T <: AnyVal, B <: AnyRef](implicit
    ev: T => B,
    tag: ClassTag[B]
  ): BoxingTag[T] = new BoxingTag[T]() {
    override type R = B
    override def clazz: Class[B] = tag.runtimeClass.asInstanceOf[Class[B]]
  }

  implicit def refBoxing[T <: AnyRef](implicit tag: ClassTag[T]): BoxingTag[T] = new BoxingTag[T] {
    override type R = T
    override def clazz: Class[T] = tag.runtimeClass.asInstanceOf[Class[T]]
  }
}
