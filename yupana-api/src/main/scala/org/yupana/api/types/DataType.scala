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

import org.threeten.extra.PeriodDuration
import org.yupana.api.types.DataType.TypeKind
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
  val internalStorable: InternalStorable[T]
  val classTag: ClassTag[T]
  val boxingTag: BoxingTag[T]
  val kind: TypeKind = TypeKind.Regular
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

  override def toString: String = s"${meta.sqlTypeName}"
}

class ArrayDataType[TT](val valueType: DataType.Aux[TT]) extends DataType {
  override type T = Seq[TT]

  override val kind: TypeKind = TypeKind.Array
  override val meta: DataTypeMeta[T] = DataTypeMeta.seqMeta(valueType.meta)
  override val storable: Storable[T] = Storable.seqStorable(valueType.storable, valueType.classTag)
  override val internalStorable: InternalStorable[Seq[TT]] =
    InternalStorable.seqStorable(valueType.internalStorable, valueType.classTag)
  override val classTag: ClassTag[T] = implicitly[ClassTag[Seq[TT]]]
  override val boxingTag: BoxingTag[Seq[TT]] = BoxingTag[Seq[TT]]

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

class TupleDataType[A, B](val aType: DataType.Aux[A], val bType: DataType.Aux[B]) extends DataType {
  override type T = (A, B)
  override val kind: TypeKind = TypeKind.Tuple
  override val meta: DataTypeMeta[T] = DataTypeMeta.tuple(aType.meta, bType.meta)
  override val storable: Storable[T] = Storable.noop
  override val internalStorable: InternalStorable[(A, B)] =
    InternalStorable.tupleStorable(aType.internalStorable, bType.internalStorable)
  override val classTag: ClassTag[T] = implicitly[ClassTag[(A, B)]]
  override val boxingTag: BoxingTag[T] = implicitly[BoxingTag[(A, B)]]
  override val ordering: Option[Ordering[(A, B)]] = None
  override val integral: Option[Integral[(A, B)]] = None
  override val fractional: Option[Fractional[(A, B)]] = None

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TupleDataType[_, _] => this.aType == that.aType && this.bType == that.bType
      case _                         => false
    }
  }

  override def hashCode(): Int = aType.hashCode() * 41 + bType.hashCode()
}

object DataType {

  sealed trait TypeKind
  object TypeKind {
    case object Regular extends TypeKind
    case object Array extends TypeKind
    case object Tuple extends TypeKind
  }

  private lazy val types = Seq(
    DataType[String],
    DataType[Double],
    DataType[Long],
    DataType[Int],
    DataType[Short],
    DataType[Byte],
    DataType[BigDecimal],
    DataType[Time],
    DataType[Blob],
    DataType[Boolean],
    DataType[Null]
  ).map(t => t.meta.sqlTypeName.toUpperCase -> t).toMap

  private val ARRAY_PREFIX = "ARRAY["
  private val ARRAY_SUFFIX = "]"

  def bySqlName(sqlName: String): Option[DataType] = {
    val upperCased = sqlName.toUpperCase
    if (!upperCased.startsWith(ARRAY_PREFIX) || !upperCased.endsWith(ARRAY_SUFFIX)) {
      types.get(upperCased)
    } else {
      val innerType = upperCased.substring(ARRAY_PREFIX.length, upperCased.length - ARRAY_SUFFIX.length)
      types.get(innerType).map(t => arrayDt(t))
    }
  }

  type Aux[TT] = DataType { type T = TT }

  def apply[T](implicit dt: DataType.Aux[T]): DataType.Aux[T] = dt

  implicit val stringDt: DataType.Aux[String] = create[String](Some(Ordering[String]), None, None)

  implicit val boolDt: DataType.Aux[Boolean] = create[Boolean](Some(Ordering[Boolean]), None, None)

  implicit val timeDt: DataType.Aux[Time] = create[Time](Some(Ordering[Time]), None, None)

  implicit val blobDt: DataType.Aux[Blob] = create[Blob](None, None, None)

  implicit val periodDt: DataType.Aux[PeriodDuration] = create[PeriodDuration](None, None, None)

  implicit def intDt[T: Storable: InternalStorable: BoxingTag: DataTypeMeta: Integral: ClassTag]: DataType.Aux[T] =
    create[T](Some(Ordering[T]), Some(implicitly[Integral[T]]), None)

  implicit def fracDt[T: Storable: InternalStorable: BoxingTag: DataTypeMeta: Fractional: ClassTag]: DataType.Aux[T] =
    create[T](Some(Ordering[T]), None, Some(implicitly[Fractional[T]]))

  implicit def tupleDt[TT, UU](implicit dtt: DataType.Aux[TT], dtu: DataType.Aux[UU]): DataType.Aux[(TT, UU)] = {
    new TupleDataType(dtt, dtu).aux
  }

  implicit def arrayDt[TT](implicit dtt: DataType.Aux[TT]): DataType.Aux[Seq[TT]] = {
    new ArrayDataType(dtt).aux
  }

  implicit val nullDt: DataType.Aux[Null] = new DataType {
    override type T = Null
    override val meta: DataTypeMeta[Null] = implicitly[DataTypeMeta[Null]]
    override val storable: Storable[Null] = Storable.noop
    override val internalStorable: InternalStorable[Null] = InternalStorable.noop
    override val classTag: ClassTag[Null] = implicitly[ClassTag[Null]]
    override val boxingTag: BoxingTag[Null] = BoxingTag[Null]
    override val ordering: Option[Ordering[Null]] = None
    override val integral: Option[Integral[Null]] = None
    override val fractional: Option[Fractional[Null]] = None
  }

  private def create[TT](o: Option[Ordering[TT]], i: Option[Integral[TT]], f: Option[Fractional[TT]])(
      implicit s: Storable[TT],
      is: InternalStorable[TT],
      m: DataTypeMeta[TT],
      ct: ClassTag[TT],
      bt: BoxingTag[TT]
  ): DataType.Aux[TT] = new DataType {
    override type T = TT
    override val meta: DataTypeMeta[T] = m
    override val storable: Storable[T] = s
    override val internalStorable: InternalStorable[TT] = is
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override val ordering: Option[Ordering[TT]] = o
    override val integral: Option[Integral[TT]] = i
    override val fractional: Option[Fractional[TT]] = f
  }

  def scaledDecimalDt(scale: Int)(
      implicit s: Storable[BigDecimal],
      is: InternalStorable[BigDecimal],
      ct: ClassTag[BigDecimal],
      bt: BoxingTag[BigDecimal]
  ): DataType.Aux[BigDecimal] = new DataType {
    override type T = BigDecimal
    override val meta: DataTypeMeta[T] = DataTypeMeta.scaledDecimalMeta(scale)
    override val storable: Storable[T] = s
    override val internalStorable: InternalStorable[BigDecimal] = is
    override val classTag: ClassTag[T] = ct
    override val boxingTag: BoxingTag[T] = bt
    override val ordering: Option[Ordering[T]] = Some(implicitly[Ordering[BigDecimal]])
    override val integral: Option[Integral[T]] = None
    override val fractional: Option[Fractional[T]] = Some(implicitly[Fractional[BigDecimal]])
  }
}
