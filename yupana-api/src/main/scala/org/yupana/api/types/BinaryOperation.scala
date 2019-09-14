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
import org.yupana.api.Time

/**
  * Binary operation definition
  * @tparam T first argument type
  */
trait BinaryOperation[T] extends Serializable {
  /** Second argument type */
  type U
  /** Result type */
  type Out

  /** Result data type */
  def dataType: DataType.Aux[Out]

  /** Function name */
  val name: String

  /** Specifies if this function should be displayed in infix notation */
  val infix: Boolean

  /**
    * Applies this function to arguments
    * @param x first argument
    * @param y second argument
    * @param binaryOperations binary operations implementation instance
    * @return calculated result
    */
  def apply(x: T, y: U)(implicit binaryOperations: BinaryOperations): Out

  override def toString: String = name
}

object BinaryOperation {
  type Aux[A, B, O] = BinaryOperation[A] { type U = B;  type Out = O }

  val PLUS = "plus"
  val MINUS = "minus"
  val MULTIPLY = "multiply"
  val DIVIDE = "divide"

  val EQ = "EQ"
  val NE = "NE"
  val GT = "GT"
  val LT = "LT"
  val GE = "GE"
  val LE = "LE"

  val CONTAINS = "contains"
  val CONTAINS_ALL = "contains_all"
  val CONTAINS_ANY = "contains_any"
  val CONTAINS_SAME = "contains_same"

  def equ[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.equ[T], "==", DataType[Boolean])
  def neq[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.neq[T], "!=", DataType[Boolean])
  def gt[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.gt[T], ">", DataType[Boolean])
  def lt[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.lt[T], "<", DataType[Boolean])
  def ge[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.ge[T], ">=", DataType[Boolean])
  def le[T: Ordering]: BinaryOperation.Aux[T, T, Boolean] = create(_.le[T], "<=", DataType[Boolean])

  def plus[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = create(_.plus[T], "+", dt)
  def minus[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = create(_.minus[T], "-", dt)
  def multiply[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = create(_.multiply[T], "*", dt)
  def divideInt[T](dt: DataType.Aux[T])(implicit num: Integral[T]): BinaryOperation.Aux[T, T, T] = create(_.intDiv[T], "/", dt)
  def divideFrac[T](dt: DataType.Aux[T])(implicit num: Fractional[T]): BinaryOperation.Aux[T, T, T] = create(_.fracDiv[T], "/", dt)

  def timeMinusTime: BinaryOperation.Aux[Time, Time, Long] = create(_.minus, "-", DataType[Long])
  def timeMinusPeriod: BinaryOperation.Aux[Time, Period, Time] = create(_.minus, "-", DataType[Time])
  def timePlusPeriod: BinaryOperation.Aux[Time, Period, Time] = create(_.plus, "+", DataType[Time])

  def periodPlusPeriod: BinaryOperation.Aux[Period, Period, Period] = create(_.plus, "+", DataType[Period])

  def contains[T]: BinaryOperation.Aux[Array[T], T, Boolean] = create(_.contains, CONTAINS, DataType[Boolean])
  def containsAll[T]: BinaryOperation.Aux[Array[T], Array[T], Boolean] = create(_.containsAll, CONTAINS_ALL, DataType[Boolean])
  def containsAny[T]: BinaryOperation.Aux[Array[T], Array[T], Boolean] = create(_.containsAny, CONTAINS_ANY, DataType[Boolean])
  def containsSame[T]: BinaryOperation.Aux[Array[T], Array[T], Boolean] = create(_.containsSame, CONTAINS_SAME, DataType[Boolean])

  private def create[A, B, O](fun: BinaryOperations => (A, B) => O, n: String, rt: DataType.Aux[O]): BinaryOperation.Aux[A, B, O] = new BinaryOperation[A] {
    override type U = B
    override type Out = O
    override val dataType: DataType.Aux[O] = rt
    override val infix: Boolean = !n.forall(_.isLetter)
    override def apply(x: A, y: B)(implicit binaryOperations: BinaryOperations): O = fun(binaryOperations)(x, y)
    override val name: String = n
  }

  def ordOperations[T: Ordering](dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[T]] = Map(
    entry(EQ, dt, BinaryOperation.equ),
    entry(NE, dt, BinaryOperation.neq),
    entry(LT, dt, BinaryOperation.lt),
    entry(GT, dt, BinaryOperation.gt),
    entry(LE, dt, BinaryOperation.le),
    entry(GE, dt, BinaryOperation.ge)
  )

  def integralOperations[T : Integral](dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[T]] = Map(
    entry(PLUS, dt, BinaryOperation.plus[T](dt)),
    entry(MINUS, dt, BinaryOperation.minus[T](dt)),
    entry(MULTIPLY, dt, BinaryOperation.multiply[T](dt)),
    entry(DIVIDE, dt, BinaryOperation.divideInt[T](dt))
  ) ++ ordOperations(dt)

  def fractionalOperations[T : Fractional](dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[T]] = Map(
    entry(PLUS, dt, BinaryOperation.plus[T](dt)),
    entry(MINUS, dt, BinaryOperation.minus[T](dt)),
    entry(MULTIPLY, dt, BinaryOperation.multiply[T](dt)),
    entry(DIVIDE, dt, BinaryOperation.divideFrac[T](dt))
  ) ++ ordOperations(dt)

  val timeOperations: Map[(String, String), BinaryOperation[Time]] = Map(
    entry(MINUS, DataType[Time], BinaryOperation.timeMinusTime),
    entry(MINUS, DataType[Period], BinaryOperation.timeMinusPeriod),
    entry(PLUS, DataType[Period], BinaryOperation.timePlusPeriod)
  ) ++ ordOperations(DataType[Time])

  val periodOperations: Map[(String, String), BinaryOperation[Period]] = Map(
    entry(PLUS, DataType[Period], BinaryOperation.periodPlusPeriod)
  )

  val stringOperations: Map[(String, String), BinaryOperation[String]] = Map(
    entry(PLUS, DataType[String], create[String, String, String](_.plus, "+", DataType[String]))
  ) ++ ordOperations(DataType[String])

  def tupleOperations[A, B](aOps: Map[(String, String), BinaryOperation[A]],
                            bOps: Map[(String, String), BinaryOperation[B]]
                           ): Map[(String, String), BinaryOperation[(A, B)]] = {
    val commonOps = aOps.keySet intersect bOps.keySet
    commonOps.map { c =>
      val ao = aOps(c)
      val bo = bOps(c)
      c -> new BinaryOperation[(A, B)] {
        override type U = (ao.U, bo.U)
        override type Out = (ao.Out, bo.Out)

        override def dataType: DataType.Aux[(ao.Out, bo.Out)] = DataType.tupleDt(ao.dataType, bo.dataType)
        override val name: String = ao.name
        override val infix: Boolean = ao.infix

        override def apply(x: (A, B), y: (ao.U, bo.U))(implicit binaryOperations: BinaryOperations): (ao.Out, bo.Out) = {
          (ao(x._1, y._1), bo(x._2, y._2))
        }
      }
    }.toMap
  }

  def arrayOperations[T](implicit dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[Array[T]]] = Map(
    entry(CONTAINS, DataType[T], contains),
    entry(CONTAINS_ALL, DataType[Array[T]], containsAll),
    entry(CONTAINS_ANY, DataType[Array[T]], containsAny),
    entry(CONTAINS_SAME, DataType[Array[T]], containsSame)
  )

  private def entry[T, U](name: String, argType: DataType.Aux[U], op: BinaryOperation.Aux[T, U, _]): ((String, String), BinaryOperation[T]) = {
    (name, argType.meta.sqlTypeName) -> op
  }
}

trait BinaryOperations {
  def equ[T: Ordering](a: T, b: T): Boolean
  def neq[T: Ordering](a: T, b: T): Boolean
  def gt[T: Ordering](a: T, b: T): Boolean
  def lt[T: Ordering](a: T, b: T): Boolean
  def ge[T: Ordering](a: T, b: T): Boolean
  def le[T: Ordering](a: T, b: T): Boolean

  def plus[N: Numeric](a: N, b: N): N
  def minus[N: Numeric](a: N, b: N): N
  def multiply[N: Numeric](a: N, b: N): N
  def intDiv[N: Integral](a: N, b: N): N
  def fracDiv[N: Fractional](a: N, b: N): N

  def minus(a: Time, b: Time): Long
  def minus(t: Time, p: Period): Time
  def plus(t: Time, p: Period): Time

  def plus(a: Period, b: Period): Period

  def plus(a: String, b: String): String

  def contains[T](a: Array[T], t: T): Boolean
  def containsAll[T](a: Array[T], b: Array[T]): Boolean
  def containsAny[T](a: Array[T], b: Array[T]): Boolean
  def containsSame[T](a: Array[T], b: Array[T]): Boolean
}
