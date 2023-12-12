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
  * Field definition
  * @param name field name
  * @param typeName field type, e.g VARCHAR, INTEGER
  */
case class ResultField(name: String, typeName: String)

/**
  * Query execution result header
  * @param id request id
  * @param tableName result table name
  * @param fields fields definitions
  */
case class ResultHeader(id: Int, tableName: String, fields: Seq[ResultField])
    extends Response[ResultHeader](ResultHeader)

object ResultHeader extends MessageHelper[ResultHeader] {
  implicit val rwResultField: ReadWrite[ResultField] =
    ReadWrite.product2[ResultField, String, String](ResultField.apply)(x => (x.name, x.typeName))

  override val tag: Byte = Tags.RESULT_HEADER
  override val readWrite: ReadWrite[ResultHeader] =
    ReadWrite.product3[ResultHeader, Int, String, Seq[ResultField]](ResultHeader.apply)(x =>
      (x.id, x.tableName, x.fields)
    )
}
