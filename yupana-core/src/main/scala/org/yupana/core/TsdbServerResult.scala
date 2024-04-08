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

import org.yupana.api.query.Result
import org.yupana.api.types.DataType
import org.yupana.core.model.BatchDataset
import org.yupana.api.utils.CloseableIterator

class TsdbServerResult(
    override val queryContext: QueryContext,
    override val data: CloseableIterator[BatchDataset]
) extends Result
    with TsdbResultBase[Iterator] {

  override def name: String = queryContext.query.table.map(_.name).getOrElse("RESULT")

  override val dataTypes: Seq[DataType] = queryContext.query.fields.map(_.expr.dataType)
  override val fieldNames: Seq[String] = nameIndex.map(_._1)

  var batch: Option[BatchDataset] = None
  var rowNumber: Int = 0

  override def next(): Boolean = {
    var f = nextRow()
    while (f && batch.exists(_.isDeleted(rowNumber))) {
      f = nextRow()
    }
    if (!f) {
      close()
    }
    f
  }

  private def nextRow(): Boolean = {
    batch match {
      case Some(b) if rowNumber < b.size - 1 =>
        rowNumber += 1
        true
      case _ if data.hasNext =>
        val nextBatch = data.next()
        batch = Some(nextBatch)
        rowNumber = 0
        nextBatch.size > 0

      case _ =>
        false
    }
  }

  override def isLast(): Boolean = {
    !data.hasNext && batch.exists(b => rowNumber >= b.size - 1)
  }

  override def close(): Unit = data.close()

  override def isEmpty(name: String): Boolean = {
    batch.get.isNull(rowNumber, dataIndexForFieldName(name))
  }

  override def isEmpty(index: Int): Boolean = {
    batch.get.isNull(rowNumber, dataIndexForFieldIndex(index))
  }

  override def get[T](name: String): T = {
    val idx = dataIndexForFieldName(name)
    internalGet(idx)
  }

  override def get[T](index: Int): T = {
    val idx = dataIndexForFieldIndex(index)
    internalGet(idx)
  }

  private def internalGet[T](idx: Int): T = {
    val expr = exprByIndex(idx)

    batch match {
      case Some(b) if b.isDefined(rowNumber, expr) => b.get(rowNumber, expr).asInstanceOf[T]
      case _                                       => null.asInstanceOf[T]
    }
  }

}
