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

package org.yupana.externallinks.universal

import java.sql.{ Blob, Clob, Date, ResultSet }

import javax.sql.DataSource
import org.yupana.api.utils.ResourceUtils.using

object JdbcUtils {
  def runQuery(
      ds: DataSource,
      q: String,
      fields: Set[String],
      params: Seq[Any]
  ): Seq[Map[String, Any]] = {
    using(ds.getConnection()) { con =>
      val ps = con.prepareStatement(q)
      params.zipWithIndex.foreach { case (p, i) => ps.setObject(i + 1, p) }
      val rs = ps.executeQuery()

      val result = Iterator
        .continually(rs)
        .takeWhile(_.next())
        .map { rs =>
          fields.map(n => n -> getResultSetValue(rs, n)).toMap
        }
        .toList

      ps.close()
      result
    }
  }

  def getResultSetValue(rs: ResultSet, name: String): Any = {
    val index = rs.findColumn(name)
    val obj = rs.getObject(index)
    val className = if (obj != null) obj.getClass.getName else null
    obj match {
      case blob: Blob =>
        blob.getBytes(1, blob.length.toInt)
      case clob: Clob =>
        clob.getSubString(1, clob.length.toInt)
      case _ =>
        if ("oracle.sql.TIMESTAMP" == className || "oracle.sql.TIMESTAMPTZ" == className) {
          rs.getTimestamp(index)
        } else if (className != null && className.startsWith("oracle.sql.DATE")) {
          val metaDataClassName = rs.getMetaData.getColumnClassName(index)
          if ("java.sql.Timestamp" == metaDataClassName || "oracle.sql.TIMESTAMP" == metaDataClassName)
            rs.getTimestamp(index)
          else rs.getDate(index)
        } else if (obj.isInstanceOf[Date] && "java.sql.Timestamp" == rs.getMetaData.getColumnClassName(index))
          rs.getTimestamp(index)
        else obj
    }
  }
}
