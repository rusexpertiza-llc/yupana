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
import org.yupana.api.utils.CloseableIterator

case class SimpleResult(
    override val name: String,
    override val fieldNames: Seq[String],
    override val dataTypes: Seq[DataType],
    rows: CloseableIterator[Array[Any]]
) extends Result {

  private val nameIndexMap = fieldNames.zipWithIndex.toMap

  private var currentRow: Option[Array[Any]] = None

  override def dataIndexForFieldName(name: String): Int = nameIndexMap(name)
  override def dataIndexForFieldIndex(idx: Int): Int = idx

  override def next(): Boolean = {
    if (rows.hasNext) {
      currentRow = Some(rows.next())
      true
    } else {
      false
    }
  }

  override def close(): Unit = rows.close()

  override def isEmpty(name: String): Boolean = {
    val idx = dataIndexForFieldName(name)
    currentRow.get(idx) == null
  }

  override def isEmpty(index: Int): Boolean = {
    val idx = dataIndexForFieldIndex(index)
    currentRow.get(idx) == null
  }

  override def get[T](name: String): T = {
    val idx = dataIndexForFieldName(name)
    currentRow.get(idx).asInstanceOf[T]
  }

  override def get[T](index: Int): T = {
    val idx = dataIndexForFieldIndex(index)
    currentRow.get(idx).asInstanceOf[T]
  }

  override def isLast(): Boolean = {
    !rows.hasNext
  }
}

object SimpleResult {
  def apply(
      name: String,
      fieldNames: Seq[String],
      dataTypes: Seq[DataType],
      rows: Iterator[Array[Any]]
  ): SimpleResult = {
    new SimpleResult(name, fieldNames, dataTypes, CloseableIterator.pure(rows))
  }
}
