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

trait Result extends Iterator[DataRow] with AutoCloseable {

  def name: String

  def fieldNames: Seq[String]
  def dataTypes: Seq[DataType]
  def dataIndexForFieldName(name: String): Int
  def dataIndexForFieldIndex(idx: Int): Int

  override def hasNext: Boolean
  override def next(): DataRow
}

object Result {
  val empty: Result = new Result {
    override val name = "EMPTY"

    override val dataTypes: Seq[DataType] = Seq.empty

    override val fieldNames: Seq[String] = Seq.empty

    override def dataIndexForFieldName(name: String): Int = 0

    override def dataIndexForFieldIndex(idx: Int): Int = 0

    override def hasNext: Boolean = false

    override def next(): DataRow = throw new IllegalStateException("Error: next on empty result")

    override def close(): Unit = ()
  }
}

class DataRow(
    val fields: Array[Any],
    dataIndexForFieldName: String => Int,
    dataIndexForFieldIndex: Int => Int
) extends Serializable {

  def isEmpty(name: String): Boolean = {
    fields(dataIndexForFieldName(name)) == null
  }

  def isEmpty(index: Int): Boolean = {
    fields(dataIndexForFieldIndex(index)) == null
  }

  def isDefined(name: String): Boolean = !isEmpty(name)

  def isDefined(index: Int): Boolean = !isEmpty(index)

  def get[T](name: String): T = {
    fields(dataIndexForFieldName(name)).asInstanceOf[T]
  }

  def get[T](index: Int): T = {
    fields(dataIndexForFieldIndex(index)).asInstanceOf[T]
  }

  def getOption[T](name: String): Option[T] = {
    Option(fields(dataIndexForFieldName(name))).asInstanceOf[Option[T]]
  }

  def getOption[T](index: Int): Option[T] = {
    Option(fields(dataIndexForFieldIndex(index))).asInstanceOf[Option[T]]
  }

  def getOrElse[T](name: String, default: => T): T = {
    val idx = dataIndexForFieldName(name)
    if (fields(idx) != null) fields(idx).asInstanceOf[T] else default
  }

  def getOrElse[T](index: Int, default: => T): T = {
    val idx = dataIndexForFieldIndex(index)
    if (fields(idx) != null) fields(idx).asInstanceOf[T] else default
  }
}
