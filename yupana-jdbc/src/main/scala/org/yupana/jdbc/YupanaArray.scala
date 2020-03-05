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

package org.yupana.jdbc

import java.sql.{ ResultSet, Array => SqlArray }
import java.util

import org.yupana.api.types.DataType

class YupanaArray[T](values: Array[T], dataType: DataType.Aux[T]) extends SqlArray {
  override def getBaseTypeName: String = dataType.meta.sqlTypeName

  override def getBaseType: Int = dataType.meta.sqlType

  override def getArray: AnyRef = values

  override def getArray(map: util.Map[String, Class[_]]): AnyRef = ???

  override def getArray(index: Long, count: Int): AnyRef = ???

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getResultSet: ResultSet = ???

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet = ???

  override def getResultSet(index: Long, count: Int): ResultSet = ???

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet = ???

  override def free(): Unit = {}
}
