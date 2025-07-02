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

package org.yupana.protocol

/**
  * Query result row
  *
  * @param queryId query id
  * @param values result values serialized into arrays of bytes
  */
case class ResultRow(queryId: Int, values: Seq[Array[Byte]]) extends Response[ResultRow](ResultRow) {
  override def toString: String = s"ResultRow(id: $queryId, values.size: ${values.size})"
}

object ResultRow extends MessageHelper[ResultRow] {
  override val tag: Tags.Tags = Tags.RESULT_ROW
  override val readWrite: ReadWrite[ResultRow] =
    ReadWrite.product2[ResultRow, Int, Seq[Array[Byte]]](ResultRow.apply)(r => (r.queryId, r.values))
}
