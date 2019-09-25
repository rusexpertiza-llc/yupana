/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.cache

import scala.reflect.ClassTag

trait BoxingTag[T] {
  type R <: AnyRef
  def clazz: Class[R]
  def cast(t: T): R
}

object BoxingTag {
  def apply[T](implicit b: BoxingTag[T]): BoxingTag[T] = b

  implicit def valBoxing[T <: AnyVal, B <: AnyRef](
      implicit
      ev: T => B,
      tag: ClassTag[B]
  ): BoxingTag[T] = new BoxingTag[T]() {
    override type R = B
    override def clazz: Class[B] = tag.runtimeClass.asInstanceOf[Class[B]]
    override def cast(t: T): B = ev(t)
  }

  implicit def refBoxing[T <: AnyRef](implicit tag: ClassTag[T]): BoxingTag[T] = new BoxingTag[T] {
    override type R = T
    override def clazz: Class[T] = tag.runtimeClass.asInstanceOf[Class[T]]
    override def cast(t: T): T = t
  }
}
