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

package org.yupana.api.query

import org.yupana.api.types.DataType

trait Result extends AutoCloseable {

  def name: String

  def fieldNames: Seq[String]
  def dataTypes: Seq[DataType]
  def dataIndexForFieldName(name: String): Int
  def dataIndexForFieldIndex(idx: Int): Int
  def next(): Boolean

  def isLast(): Boolean

  def isEmpty(name: String): Boolean

  def isEmpty(index: Int): Boolean

  def isDefined(name: String): Boolean = !isEmpty(name)

  def isDefined(index: Int): Boolean = !isEmpty(index)

  def get[T](name: String): T

  def get[T](index: Int): T

  def getOption[T](name: String): Option[T] = {
    if (isEmpty(name)) None else Some(get[T](name))
  }

  def getOption[T](index: Int): Option[T] = {
    if (isEmpty(index)) None else Some(get[T](index))
  }

  def getOrElse[T](name: String, default: => T): T = {
    if (isEmpty(name)) default else get(name)
  }

  def getOrElse[T](index: Int, default: => T): T = {
    if (isEmpty(index)) default else get(index)
  }

}

object Result {
  val empty: Result = new Result {
    override val name = "EMPTY"

    override val dataTypes: Seq[DataType] = Seq.empty

    override val fieldNames: Seq[String] = Seq.empty

    override def dataIndexForFieldName(name: String): Int = 0

    override def dataIndexForFieldIndex(idx: Int): Int = 0

    override def next(): Boolean = false

    override def close(): Unit = ()

    override def isEmpty(name: String): Boolean = true

    override def isEmpty(index: Int): Boolean = true

    override def get[T](name: String): T = throw new IllegalStateException("get on empty result")

    override def get[T](index: Int): T = throw new IllegalStateException("get on empty result")

    override def getOption[T](name: String): Option[T] = None

    override def getOption[T](index: Int): Option[T] = None

    override def getOrElse[T](name: String, default: => T): T = default

    override def getOrElse[T](index: Int, default: => T): T = default

    override def isLast(): Boolean = true
  }
}
