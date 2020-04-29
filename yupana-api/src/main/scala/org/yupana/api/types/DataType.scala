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

package org.yupana.api.types

import org.joda.time.Period
import org.yupana.api.{ Blob, Time }

import scala.reflect.ClassTag

/**
  * Data type definition. Contains information about processing data type: type metadata, serialization, available operations
  * on this type.
  */
trait DataType extends Serializable {
  type T
  val meta: DataTypeMeta[T]
  val storable: Storable[T]
  val classTag: ClassTag[T]
  val boxingTag: BoxingTag[T]
  val isArray: Boolean = false
  def operations: TypeOperations[T]

  def aux: DataType.Aux[T] = this.asInstanceOf[DataType.Aux[T]]

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) false
    else
      obj match {
        case that: DataType =>
          this.meta == that.meta
        case _ => false
      }
  }

  override def toString: String = s"${meta.sqlTypeName}"
}

class ArrayDataType[TT](val valueType: DataType.Aux[TT]) extends DataType {
  override type T = Array[TT]

  override val isArray: Boolean = true
  override val meta: DataTypeMeta[T] = DataTypeMeta.arrayMeta(valueType.meta)
  override val storable: Storable[T] = Storable.arrayStorable(valueType.storable, valueType.classTag)
  override val classTag: ClassTag[T] = valueType.classTag.wrap
  override val boxingTag: BoxingTag[Array[TT]] = BoxingTag.arrayBoxing(valueType.classTag)

  override def operations: TypeOperations[T] = TypeOperations.arrayOperations(valueType)
}

object DataType {
  private lazy val types = Seq(
    DataType[String],
    DataType[Double],
    DataType[Long],
    DataType[Int],
    DataType[Short],
    DataType[Byte],
    DataType[BigDecimal],
    DataType[Time],
    DataType[Boolean]
  ).map(t => t.meta.sqlTypeName -> t).toMap

  private val ARRAY_PREFIX = "ARRAY["
  private val ARRAY_SUFFIX = "]"

  def bySqlName(sqlName: String): Option[DataType] = {
    if (!sqlName.startsWith(ARRAY_PREFIX) || !sqlName.endsWith(ARRAY_SUFFIX)) {
      types.get(sqlName)
    } else {
      val innerType = sqlName.substring(ARRAY_PREFIX.length, sqlName.length - ARRAY_SUFFIX.length)
      types.get(innerType).map(t => arrayDt(t))
    }
  }

  type Aux[TT] = DataType { type T = TT }

  def apply[T](implicit dt: DataType.Aux[T]): DataType.Aux[T] = dt

  implicit val stringDt: DataType.Aux[String] = DataType[String](TypeOperations.stringOperations)

  implicit val boolDt: DataType.Aux[Boolean] = DataType[Boolean](TypeOperations.boolOperations)

  implicit val timeDt: DataType.Aux[Time] = DataType[Time](TypeOperations.timeOperations)

  implicit val periodDt: DataType.Aux[Period] = DataType[Period](TypeOperations.periodOperations)

  implicit val blobDt: DataType.Aux[Blob] = DataType[Blob](TypeOperations.blobOperations)

  implicit def intDt[T: Storable: BoxingTag: DataTypeMeta: Integral: ClassTag]: DataType.Aux[T] =
    DataType[T]((r: DataType.Aux[T]) => TypeOperations.intOperations(r))

  implicit def fracDt[T: Storable: BoxingTag: DataTypeMeta: Fractional: ClassTag]: DataType.Aux[T] =
    DataType[T]((r: DataType.Aux[T]) => TypeOperations.fracOperations(r))

  implicit def tupleDt[TT, UU](implicit dtt: DataType.Aux[TT], dtu: DataType.Aux[UU]): DataType.Aux[(TT, UU)] = {
    new DataType {
      override type T = (TT, UU)
      override val meta: DataTypeMeta[T] = DataTypeMeta.tuple(dtt.meta, dtu.meta)
      override val storable: Storable[T] = Storable.noop
      override val classTag: ClassTag[T] = implicitly[ClassTag[(TT, UU)]]
      override val boxingTag: BoxingTag[T] = implicitly[BoxingTag[(TT, UU)]]

      override def operations: TypeOperations[T] = TypeOperations.tupleOperations(dtt, dtu)
    }
  }

  implicit def arrayDt[TT](implicit dtt: DataType.Aux[TT]): DataType.Aux[Array[TT]] = {
    new ArrayDataType(dtt).aux
  }

  private def apply[TT](ops: TypeOperations[TT])(
      implicit
      s: Storable[TT],
      m: DataTypeMeta[TT],
      ct: ClassTag[TT],
      bt: BoxingTag[TT]
  ): DataType.Aux[TT] = new DataType {
    override type T = TT
    override val meta: DataTypeMeta[T] = m
    override val storable: Storable[T] = s
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override lazy val operations: TypeOperations[TT] = ops
  }

  private def apply[TT](getOps: DataType.Aux[TT] => TypeOperations[TT])(
      implicit
      s: Storable[TT],
      m: DataTypeMeta[TT],
      ct: ClassTag[TT],
      bt: BoxingTag[TT]
  ): DataType.Aux[TT] = new DataType {
    override type T = TT
    override val meta: DataTypeMeta[T] = m
    override val storable: Storable[T] = s
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override lazy val operations: TypeOperations[TT] = getOps(this)
  }
}
