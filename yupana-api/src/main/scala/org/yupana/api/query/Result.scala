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

import scala.collection.immutable

trait Result extends immutable.Iterable[DataRow] {

  def fieldNames: Seq[String]
  def dataTypes: Seq[DataType]
  def dataIndexForFieldName(name: String): Int
  def dataIndexForFieldIndex(idx: Int): Int
  def rows: Iterator[Array[Option[Any]]]

  override def iterator: Iterator[DataRow] =
    rows.map(r => new DataRow(r, dataIndexForFieldName, dataIndexForFieldIndex))

  override def size: Int = rows.size
}

object Result {
  val empty: Result = new Result {

    override val dataTypes: Seq[DataType] = Seq.empty

    override val fieldNames: Seq[String] = Seq.empty

    override def dataIndexForFieldName(name: String): Int = 0

    override def dataIndexForFieldIndex(idx: Int): Int = 0

    override def rows: Iterator[Array[Option[Any]]] = Iterator.empty
  }
}

case class SimpleResult(fieldNames: Seq[String], dataTypes: Seq[DataType], rows: Iterator[Array[Option[Any]]])
    extends Result {

  private val nameIndexMap = fieldNames.zipWithIndex.toMap

  override def dataIndexForFieldName(name: String): Int = nameIndexMap(name)
  override def dataIndexForFieldIndex(idx: Int): Int = idx
}

class DataRow(
    val fields: Array[Option[Any]],
    dataIndexForFieldName: String => Int,
    dataIndexForFieldIndex: Int => Int
) {

  def fieldValueByName[T](name: String): Option[T] = {
    fields(dataIndexForFieldName(name)).asInstanceOf[Option[T]]
  }

  def fieldByIndex[T](index: Int): Option[T] = {
    fields(dataIndexForFieldIndex(index)).asInstanceOf[Option[T]]
  }
}
