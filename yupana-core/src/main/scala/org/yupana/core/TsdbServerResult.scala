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

package org.yupana.core

import org.yupana.api.query.{ DataRow, Result }
import org.yupana.api.types.DataType
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.api.utils.CloseableIterator

class TsdbServerResult(
    override val queryContext: QueryContext,
    override val internalRowBuilder: InternalRowBuilder,
    data: CloseableIterator[InternalRow]
) extends Result
    with TsdbResultBase[Iterator] {

  override def name: String = queryContext.query.table.map(_.name).getOrElse("RESULT")

  override def rows: CloseableIterator[InternalRow] = data

  override val dataTypes: Seq[DataType] = queryContext.query.fields.map(_.expr.dataType)
  override val fieldNames: Seq[String] = nameIndex.map(_._1)

  override def hasNext: Boolean = data.hasNext

  override def next(): DataRow = {
    val row = rows.next()
    dataRow(row)
  }

  override def close(): Unit = rows.close()
}
