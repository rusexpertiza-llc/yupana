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

package org.yupana.api.schema

import org.yupana.api.types.{ DataType, FixedStorable, Storable }
import org.yupana.api.utils.DimOrdering

import scala.reflect.ClassTag

sealed trait Dimension {
  type T
  type R

  def rStorable: FixedStorable[R]
  def tOrdering: DimOrdering[T]
  def rOrdering: DimOrdering[R]

  def rCt: ClassTag[R]

  def name: String
  def dataType: DataType.Aux[T]

  def aux: Dimension.Aux2[T, R] = this
}

object Dimension {
  type Aux[TT] = Dimension { type T = TT }
  type AuxR[RR] = Dimension { type R = RR }
  type Aux2[TT, RR] = Dimension { type T = TT; type R = RR }
}

case class DictionaryDimension(override val name: String, hashFunction: Option[String => Int] = None)
    extends Dimension {

  override type T = String
  override type R = Long
  override val rCt: ClassTag[Long] = implicitly[ClassTag[Long]]

  override def rStorable: FixedStorable[Long] = FixedStorable[Long]
  override def tOrdering: DimOrdering[String] = implicitly[DimOrdering[String]]
  override def rOrdering: DimOrdering[Long] = implicitly[DimOrdering[Long]]

  override val dataType: DataType.Aux[T] = DataType[String]

  def hash(v: String): Int = _hash(v)

  private val _hash: String => Int = hashFunction.getOrElse(zeroHash)

  private def zeroHash(s: String): Int = 0

  override def toString: String = s"DicDimension($name)"

  override def hashCode(): Int = name.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case DictionaryDimension(n, _) => name == n
    case _                         => false
  }
}

case class RawDimension[TT](override val name: String)(
    implicit val rStorable: FixedStorable[TT],
    val rOrdering: DimOrdering[TT],
    val rCt: ClassTag[TT],
    dt: DataType.Aux[TT]
) extends Dimension {
  override type T = TT
  override type R = TT

  override def tOrdering: DimOrdering[TT] = rOrdering

  override val dataType: DataType.Aux[T] = dt

  override def hashCode(): Int = name.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case RawDimension(n) => name == n
    case _               => false
  }

  override def toString: String = s"RawDimension($name)"
}

case class HashDimension[TT, RR](override val name: String, hashFunction: TT => RR)(
    implicit val rStorable: FixedStorable[RR],
    implicit val tStorable: Storable[TT],
    val rOrdering: DimOrdering[RR],
    val tOrdering: DimOrdering[TT],
    val rCt: ClassTag[RR],
    dt: DataType.Aux[TT]
) extends Dimension {

  override type T = TT
  override type R = RR

  override def dataType: DataType.Aux[TT] = dt

  override def hashCode(): Int = name.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case HashDimension(n, _) => name == n
    case _                   => false
  }

  override def toString: String = s"HashDimension($name)"
}
