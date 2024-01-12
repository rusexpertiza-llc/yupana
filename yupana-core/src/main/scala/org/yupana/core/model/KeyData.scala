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

package org.yupana.core.model

import org.yupana.api.query.Expression

import java.io.{ ObjectInputStream, ObjectOutputStream }
import org.yupana.core.QueryContext

import scala.util.hashing.MurmurHash3

class KeyData(
    @transient val queryContext: QueryContext,
    @transient val rowBuilder: InternalRowBuilder @transient,
    val row: InternalRow
) extends Serializable {

  private var data: Array[Any] = _

  // TODO: row.get[Any] - fix it!
  override def hashCode(): Int = {
    import scala.util.hashing.MurmurHash3._

    if (queryContext != null) {
      val h = queryContext.groupByIndices.foldLeft(MurmurHash3.arraySeed) {
        case (h, (expr, idx)) =>
          val value = row.get(rowBuilder, idx)(expr.dataType.internalStorable)
          mix(h, value.##)
      }
      finalizeHash(h, queryContext.groupByIndices.length)
    } else {
      arrayHash(data)
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: KeyData =>
        if (this eq that) {
          true
        } else if (this.queryContext != null && that.queryContext != null) {
          queryContext.groupByIndices.forall {
            case (expr, idx) =>
              val v1 = this.row.get(rowBuilder, idx)(expr.dataType.internalStorable)
              val v2 = that.row.get(rowBuilder, idx)(expr.dataType.internalStorable)
              v1 == v2
          }
        } else if (this.queryContext != null) {
          queryContext.groupByIndices.indices.forall { i =>
            val (expr: Expression[Any], idx) = queryContext.groupByIndices(i)
            val v1 = this.row.get(rowBuilder, idx)(expr.dataType.internalStorable)
            val v2 = that.data(i)
            v1 == v2
          }
        } else if (that.queryContext != null) {
          that.queryContext.groupByIndices.indices.forall { i =>
            val (expr: Expression[Any], idx) = that.queryContext.groupByIndices(i)
            val v1 = this.data(i)
            val v2 = that.row.get(that.rowBuilder, idx)(expr.dataType.internalStorable)
            v1 == v2
          }
        } else {
          this.data sameElements that.data
        }

      case _ => false
    }
  }

  private def calcData: Array[Any] = {
    val keyData = Array.ofDim[Any](queryContext.groupByIndices.length)

    keyData.indices foreach { i =>
      val (expr: Expression[Any], idx) = queryContext.groupByIndices(i)
      keyData(i) = row.get(rowBuilder, idx)(expr.dataType.internalStorable)
    }

    keyData
  }

  // Overriding Java serialization
  private def writeObject(oos: ObjectOutputStream): Unit = {
    if (data == null) data = calcData
    oos.writeObject(data)
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    data = ois.readObject().asInstanceOf[Array[Any]]
  }
}
