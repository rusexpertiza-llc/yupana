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

  override def dataIndexForFieldName(name: String): Int = nameIndexMap(name)
  override def dataIndexForFieldIndex(idx: Int): Int = idx

  override def hasNext: Boolean = rows.hasNext

  override def next(): DataRow = new DataRow(rows.next(), dataIndexForFieldName, dataIndexForFieldIndex)

  override def close(): Unit = rows.close()
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
