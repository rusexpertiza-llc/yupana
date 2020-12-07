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
import org.yupana.api.{ Blob, Time, TypeMeta }

import scala.reflect.ClassTag

/**
  * Data type definition. Contains information about processing data type: type metadata, serialization, available operations
  * on this type.
  */
trait DataType extends Serializable {
  type T
  val sqlTypeName: String
  val scale: Int
  val storable: Storable[T]
  val classTag: ClassTag[T]
  val boxingTag: BoxingTag[T]
  val isArray: Boolean = false
  val ordering: Option[Ordering[T]]
  val integral: Option[Integral[T]]
  val fractional: Option[Fractional[T]]
  def numeric: Option[Numeric[T]] = integral orElse fractional

  def aux: DataType.Aux[T] = this.asInstanceOf[DataType.Aux[T]]

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: DataType => this.classTag == that.classTag
      case _              => false
    }
  }

  override def hashCode(): Int = this.classTag.hashCode()

  override def toString: String = s"$sqlTypeName"
}

class ArrayDataType[TT](val valueType: DataType.Aux[TT]) extends DataType {
  override type T = Seq[TT]

  override val isArray: Boolean = true
  override val sqlTypeName: String = SqlTypeName.seqName(SqlTypeName(valueType.sqlTypeName)).sqlTypeName
  override val storable: Storable[T] = Storable.seqStorable(valueType.storable, valueType.classTag)
  override val classTag: ClassTag[T] = implicitly[ClassTag[Seq[TT]]]
  override val boxingTag: BoxingTag[Seq[TT]] = BoxingTag[Seq[TT]]
  override val scale: Int = 0

  override val ordering: Option[Ordering[Seq[TT]]] = None
  override val integral: Option[Integral[Seq[TT]]] = None
  override val fractional: Option[Fractional[Seq[TT]]] = None

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ArrayDataType[_] => this.valueType == that.valueType
      case _                      => false
    }
  }

  override def hashCode(): Int = (37 * 17 + classTag.hashCode()) * 17 + valueType.classTag.hashCode()
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
  ).map(t => t.sqlTypeName -> t).toMap

  def bySqlName(sqlName: String): Option[DataType] = {
    if (!sqlName.startsWith(TypeMeta.ARRAY_PREFIX) || !sqlName.endsWith(TypeMeta.ARRAY_SUFFIX)) {
      types.get(sqlName)
    } else {
      val innerType = sqlName.substring(TypeMeta.ARRAY_PREFIX.length, sqlName.length - TypeMeta.ARRAY_SUFFIX.length)
      types.get(innerType).map(t => arrayDt(t))
    }
  }

  type Aux[TT] = DataType { type T = TT }

  def apply[T](implicit dt: DataType.Aux[T]): DataType.Aux[T] = dt

  implicit val stringDt: DataType.Aux[String] = create[String](Some(Ordering[String]), None, None)

  implicit val boolDt: DataType.Aux[Boolean] = create[Boolean](Some(Ordering[Boolean]), None, None)

  implicit val timeDt: DataType.Aux[Time] = create[Time](Some(Ordering[Time]), None, None)

  implicit val blobDt: DataType.Aux[Blob] = create[Blob](None, None, None)

  implicit val periodDt: DataType.Aux[Period] = create[Period](None, None, None)

  implicit def intDt[T: Storable: BoxingTag: SqlTypeName: Integral: ClassTag]: DataType.Aux[T] =
    create[T](Some(Ordering[T]), Some(implicitly[Integral[T]]), None)

  implicit def fracDt[T: Storable: BoxingTag: SqlTypeName: Fractional: ClassTag]: DataType.Aux[T] =
    create[T](Some(Ordering[T]), None, Some(implicitly[Fractional[T]]))

  implicit def tupleDt[TT, UU](implicit dtt: DataType.Aux[TT], dtu: DataType.Aux[UU]): DataType.Aux[(TT, UU)] = {
    new DataType {
      override type T = (TT, UU)

      override val sqlTypeName: String = s"${dtt.sqlTypeName}_${dtu.sqlTypeName}"
      override val scale: Int = 0
      override val storable: Storable[T] = Storable.noop
      override val classTag: ClassTag[T] = implicitly[ClassTag[(TT, UU)]]
      override val boxingTag: BoxingTag[T] = implicitly[BoxingTag[(TT, UU)]]
      override val ordering: Option[Ordering[(TT, UU)]] = None
      override val integral: Option[Integral[(TT, UU)]] = None
      override val fractional: Option[Fractional[(TT, UU)]] = None
    }
  }

  implicit def arrayDt[TT](implicit dtt: DataType.Aux[TT]): DataType.Aux[Seq[TT]] = {
    new ArrayDataType(dtt).aux
  }

  implicit val nullDt: DataType.Aux[Null] = new DataType {
    override type T = Null
    override val sqlTypeName: String = implicitly[SqlTypeName[Null]].sqlTypeName
    override val scale: Int = 0
    override val storable: Storable[Null] = Storable.noop
    override val classTag: ClassTag[Null] = implicitly[ClassTag[Null]]
    override val boxingTag: BoxingTag[Null] = BoxingTag[Null]
    override val ordering: Option[Ordering[Null]] = None
    override val integral: Option[Integral[Null]] = None
    override val fractional: Option[Fractional[Null]] = None
  }

  private def create[TT](
      o: Option[Ordering[TT]],
      i: Option[Integral[TT]],
      f: Option[Fractional[TT]]
  )(
      implicit
      name: SqlTypeName[TT],
      s: Storable[TT],
      ct: ClassTag[TT],
      bt: BoxingTag[TT]
  ): DataType.Aux[TT] = new DataType {
    override type T = TT
    override val sqlTypeName: String = name.sqlTypeName
    override val scale: Int = 0
    override val storable: Storable[T] = s
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override val ordering: Option[Ordering[TT]] = o
    override val integral: Option[Integral[TT]] = i
    override val fractional: Option[Fractional[TT]] = f
  }

  def scaledDecimalDt(customScale: Int)(
      implicit
      n: SqlTypeName[BigDecimal],
      s: Storable[BigDecimal],
      ct: ClassTag[BigDecimal],
      bt: BoxingTag[BigDecimal]
  ): DataType.Aux[BigDecimal] = new DataType {
    override type T = BigDecimal
    override val sqlTypeName: String = n.sqlTypeName
    override val scale: Int = customScale
    override val storable: Storable[T] = s
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override val ordering: Option[Ordering[T]] = Some(implicitly[Ordering[BigDecimal]])
    override val integral: Option[Integral[T]] = None
    override val fractional: Option[Fractional[T]] = Some(implicitly[Fractional[BigDecimal]])
  }
}
