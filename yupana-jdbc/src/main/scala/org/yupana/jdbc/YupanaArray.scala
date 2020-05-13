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

import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType

class YupanaArray[T](name: String, values: Array[T], valueType: DataType.Aux[T]) extends SqlArray {
  override def getBaseTypeName: String = valueType.meta.sqlTypeName

  override def getBaseType: Int = valueType.meta.sqlType

  override def getArray: Array[T] = values

  override def getArray(map: util.Map[String, Class[_]]): Array[T] = {
    JdbcUtils.checkTypeMapping(map)
    getArray
  }

  override def getArray(index: Long, count: Int): Array[T] = {
    val start = (index - 1).toInt
    values.slice(start, start + count)
  }

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): AnyRef = {
    JdbcUtils.checkTypeMapping(map)
    getArray(index, count)
  }

  override def getResultSet: ResultSet = {
    createResultSet(values, 1)
  }

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet = {
    JdbcUtils.checkTypeMapping(map)
    getResultSet
  }

  override def getResultSet(index: Long, count: Int): ResultSet = {
    createResultSet(getArray(index, count), index.toInt)
  }

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet = {
    JdbcUtils.checkTypeMapping(map)
    getResultSet(index, count)
  }

  override def free(): Unit = {}

  private def createResultSet(array: Array[T], startIndex: Int): ResultSet = {
    val it = array.zip(Stream.from(startIndex)).map { case (v, i) => Array[Option[Any]](Some(i), Option(v)) }.toIterator
    new YupanaResultSet(null, new SimpleResult(name, Seq("INDEX", "VALUE"), Seq(DataType[Int], valueType), it))
  }
}
