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

class TsdbServerResult(override val queryContext: QueryContext, data: Iterator[Array[Option[Any]]])
    extends Result
    with TsdbResultBase[Iterator] {

  override def name: String = queryContext.query.table.name

  override def rows: Iterator[Array[Option[Any]]] = data

  override val dataTypes: Seq[DataType] = queryContext.query.fields.map(_.expr.dataType)
  override val fieldNames: Seq[String] = nameIndex.map(_._1)
}
