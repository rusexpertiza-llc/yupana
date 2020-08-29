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

package org.yupana.core.sql

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.types.DataType

object FunctionRegistry {
  import scala.language.higherKinds

  type ArrayExpr[T] = Expression.Aux[Array[T]]

  case class FunctionDesc(names: Set[String], f: Expression => Either[String, Expression])
  case class Function2Desc(names: Set[String], f: (Expression, Expression) => Either[String, Expression])

  trait Bind[A[_], Z] {
    def apply[T](a: A[T]): Z
  }

  trait Bind2[A[_], B[_], Z] {
    def apply[T](a: A[T], b: B[T]): Z
  }

  trait Bind3[A[_], B[_], C[_], Z] {
    def apply[T](a: A[T], b: B[T], c: C[T]): Z
  }

  private val unaryFunctions: List[FunctionDesc] = List(
    // AGGREGATES
    uNum("sum", new Bind2[Expression.Aux, Numeric, Expression] {
      override def apply[T](e: Expression.Aux[T], n: Numeric[T]): Expression = SumExpr(e)(n)
    }),
    uOrd("min", new Bind2[Expression.Aux, Ordering, Expression] {
      override def apply[T](e: Expression.Aux[T], o: Ordering[T]): Expression = MinExpr(e)(o)
    }),
    uOrd("max", new Bind2[Expression.Aux, Ordering, Expression] {
      override def apply[T](e: Expression.Aux[T], o: Ordering[T]): Expression = MaxExpr(e)(o)
    }),
    uAny("count", e => CountExpr(e.aux)),
    uAny("distinct_count", e => DistinctCountExpr(e.aux)),
    uAny("distinct_random", e => DistinctRandomExpr(e.aux)),
    // WINDOW
    uAny("lag", e => LagExpr(e.aux)),
    // REAL UNARY
    uNum("-", new Bind2[Expression.Aux, Numeric, Expression] {
      override def apply[T](e: Expression.Aux[T], n: Numeric[T]): Expression = UnaryMinusExpr(e)(n)
    }),
    uNum("abs", new Bind2[Expression.Aux, Numeric, Expression] {
      override def apply[T](e: Expression.Aux[T], n: Numeric[T]): Expression = AbsExpr(e)(n)
    }),
    uTyped(Set("year", "trunc_year"), TruncYearExpr),
    uTyped(Set("month", "trunc_month"), TruncMonthExpr),
    uTyped(Set("week", "trunc_week"), TruncWeekExpr),
    uTyped(Set("day", "trunc_day"), TruncDayExpr),
    uTyped(Set("hour", "trunc_hour"), TruncHourExpr),
    uTyped(Set("minute", "trunc_minute"), TruncMinuteExpr),
    uTyped(Set("second", "trunc_second"), TruncSecondExpr),
    uTyped("extract_year", ExtractYearExpr),
    uTyped("extract_month", ExtractMonthExpr),
    uTyped("extract_day", ExtractDayExpr),
    uTyped("extract_hour", ExtractHourExpr),
    uTyped("extract_minute", ExtractMinuteExpr),
    uTyped("extract_second", ExtractSecondExpr),
    uTyped("length", LengthExpr),
    uAny("is_null", e => IsNullExpr(e.aux)),
    uAny("is_not_null", e => IsNotNullExpr(e.aux)),
    uTyped("not", NotExpr),
    uTyped("tokens", TokensExpr),
    uTyped("split", SplitExpr),
    uTyped("lower", LowerExpr),
    uTyped("upper", UpperExpr),
    uArray(
      "array_to_string",
      new Bind[ArrayExpr, Expression] {
        override def apply[T](a: Expression.Aux[Array[T]]): Expression = ArrayToStringExpr(a)
      }
    ),
    uTyped("tokens", ArrayTokensExpr),
    // SPECIAL
    FunctionDesc(Set("id"), createDimIdExpr)
  )

  private val binaryFunctions: List[Function2Desc] = List(
    biOrd(
      ">=",
      new Bind3[Expression.Aux, Expression.Aux, Ordering, Condition] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], o: Ordering[T]): Condition = GeExpr(a, b)(o)
      }
    ),
    biOrd(
      ">",
      new Bind3[Expression.Aux, Expression.Aux, Ordering, Condition] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], o: Ordering[T]): Condition = GtExpr(a, b)(o)
      }
    ),
    biOrd(
      "<=",
      new Bind3[Expression.Aux, Expression.Aux, Ordering, Condition] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], o: Ordering[T]): Condition = LeExpr(a, b)(o)
      }
    ),
    biOrd(
      "<",
      new Bind3[Expression.Aux, Expression.Aux, Ordering, Condition] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], o: Ordering[T]): Condition = LtExpr(a, b)(o)
      }
    ),
    biSame(
      "=",
      new Bind2[Expression.Aux, Expression.Aux, Expression] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T]): Condition = EqExpr(a, b)
      }
    ),
    biSame(
      "<>",
      new Bind2[Expression.Aux, Expression.Aux, Expression] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T]): Condition = NeqExpr(a, b)
      }
    ),
    biNum(
      "+",
      new Bind3[Expression.Aux, Expression.Aux, Numeric, Expression] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], n: Numeric[T]): Expression = PlusExpr(a, b)(n)
      }
    ),
    biNum(
      "-",
      new Bind3[Expression.Aux, Expression.Aux, Numeric, Expression] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], n: Numeric[T]): Expression =
          MinusExpr(a, b)(n)
      }
    ),
    biNum(
      "*",
      new Bind3[Expression.Aux, Expression.Aux, Numeric, Expression] {
        override def apply[T](a: Expression.Aux[T], b: Expression.Aux[T], n: Numeric[T]): Expression =
          TimesExpr(a, b)(n)
      }
    ),
    Function2Desc(
      Set("/"),
      (a: Expression, b: Expression) =>
        ExprPair.alignTypes(a, b) match {
          case Right(pair) if pair.dataType.integral.isDefined =>
            Right(DivIntExpr(pair.a, pair.b)(pair.dataType.integral.get))
          case Right(pair) if pair.dataType.fractional.isDefined =>
            Right(DivFracExpr(pair.a, pair.b)(pair.dataType.fractional.get))
          case Right(pair) => Left(s"Cannot apply math operations to ${pair.dataType}")
          case Left(msg)   => Left(msg)
        }
    ),
    biTyped("+", ConcatExpr),
    biTyped("-", TimeMinusExpr),
    biTyped("-", TimeMinusPeriodExpr),
    biTyped("+", TimePlusPeriodExpr),
    biTyped("+", PeriodPlusPeriodExpr),
    biArray("contains_any", new Bind2[ArrayExpr, ArrayExpr, Expression] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression = ContainsAnyExpr(a, b)
    }),
    biArray("contains_all", new Bind2[ArrayExpr, ArrayExpr, Expression] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression = ContainsAnyExpr(a, b)
    }),
    biArray("contains_same", new Bind2[ArrayExpr, ArrayExpr, Expression] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression = ContainsSameExpr(a, b)
    })
  )

  def unary(name: String, e: Expression): Either[String, Expression] = {
    unaryFunctions.filter(_.names.contains(name.toLowerCase)) match {
      case Nil => Left(s"Undefined function $name")
      case xs =>
        val (r, l) = xs.map(_.f(e)).partition(_.isRight)
        if (r.isEmpty) {
          Left(l.collect { case Left(m) => m } mkString ",")
        } else if (r.size > 1) {
          Left(s"Ambiguous function '$name' call on $e")
        } else {
          r.head
        }
    }
  }

  def bi(name: String, a: Expression, b: Expression): Either[String, Expression] = {
    binaryFunctions.filter(_.names.contains(name.toLowerCase)) match {
      case Nil => Left(s"Undefined function $name")
      case xs =>
        val (r, l) = xs.map(_.f(a, b)).partition(_.isRight)
        if (r.isEmpty) {
          Left(l.collect { case Left(m) => m } mkString ",")
        } else if (r.size > 1) {
          Left(s"Ambiguous function '$name' call on ($a, $b)")
        } else {
          r.head
        }
    }
  }

  private def uNum(
      fn: String,
      create: Bind2[Expression.Aux, Numeric, Expression]
  ): FunctionDesc = {
    FunctionDesc(
      Set(fn),
      e =>
        e.dataType.numeric match {
          case Some(num) => Right(create(e.aux, num))
          case None      => Left(s"$fn requires a number, but got ${e.dataType}")
        }
    )
  }

  private def uOrd(
      fn: String,
      create: Bind2[Expression.Aux, Ordering, Expression]
  ): FunctionDesc = {
    FunctionDesc(
      Set(fn),
      e =>
        e.dataType.ordering match {
          case Some(ord) => Right(create(e.aux, ord))
          case None      => Left(s"$fn cannot be applied to ${e.dataType}")
        }
    )
  }

  private def uAny(fn: String, create: Expression => Expression): FunctionDesc =
    FunctionDesc(Set(fn), e => Right(create(e)))

  private def uTyped[T](fn: String, create: Expression.Aux[T] => Expression)(
      implicit dt: DataType.Aux[T]
  ): FunctionDesc = uTyped(Set(fn), create)

  private def uTyped[T](fns: Set[String], create: Expression.Aux[T] => Expression)(
      implicit dt: DataType.Aux[T]
  ): FunctionDesc = {
    FunctionDesc(
      fns,
      e =>
        if (e.dataType == dt) Right(create(e.asInstanceOf[Expression.Aux[T]]))
        else Left(s"Function ${fns.head} cannot be applied to $e of type ${e.dataType}")
    )
  }

  private def uArray[T](fn: String, create: Bind[ArrayExpr, Expression]): FunctionDesc = {
    FunctionDesc(
      Set(fn),
      e =>
        if (e.dataType.isArray) Right(create(e.asInstanceOf[Expression.Aux[Array[T]]]))
        else Left(s"Function $fn requires array, but got $e")
    )
  }

  private def biOrd(
      fn: String,
      create: Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition]
  ): Function2Desc = {
    Function2Desc(
      Set(fn),
      (a, b) =>
        ExprPair.alignTypes(a, b) match {
          case Right(pair) if pair.dataType.ordering.isDefined =>
            Right(create(pair.a, pair.b, pair.dataType.ordering.get))
          case Right(_)  => Left(s"Cannot compare types ${a.dataType} and ${b.dataType}")
          case Left(msg) => Left(msg)

        }
    )
  }

  private def biNum(
      fn: String,
      create: Bind3[Expression.Aux, Expression.Aux, Numeric, Expression]
  ): Function2Desc = {
    Function2Desc(
      Set(fn),
      (a, b) =>
        ExprPair.alignTypes(a, b) match {
          case Right(pair) if pair.dataType.numeric.isDefined =>
            Right(create(pair.a, pair.b, pair.dataType.numeric.get))
          case Right(_)  => Left(s"Cannot apply $fn to ${a.dataType} and ${b.dataType}")
          case Left(msg) => Left(msg)

        }
    )
  }

  private def biSame(fn: String, create: Bind2[Expression.Aux, Expression.Aux, Expression]): Function2Desc = {
    Function2Desc(
      Set(fn),
      (a, b) => ExprPair.alignTypes(a, b).right.map(pair => create(pair.a, pair.b))
    )
  }

  private def biTyped[T, U](fn: String, create: (Expression.Aux[T], Expression.Aux[U]) => Expression)(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): Function2Desc = Function2Desc(
    Set(fn),
    (a, b) =>
      if (a.dataType == dtt && b.dataType == dtu)
        Right(create(a.asInstanceOf[Expression.Aux[T]], b.asInstanceOf[Expression.Aux[U]]))
      else Left(s"Function $fn cannot be applied to $a, $b of types ${a.dataType}, ${b.dataType}")
  )

  private def biArray[T](fn: String, create: Bind2[ArrayExpr, ArrayExpr, Expression]): Function2Desc = {
    Function2Desc(
      Set(fn),
      (a, b) =>
        if (a.dataType.isArray && b.dataType.isArray)
          Right(create(a.asInstanceOf[Expression.Aux[Array[T]]], b.asInstanceOf[Expression.Aux[Array[T]]]))
        else Left(s"Function $fn requires array, but got $a, $b")
    )
  }

  private def createDimIdExpr(expr: Expression): Either[String, Expression] = {
    expr match {
      case DimensionExpr(dim)            => Right(DimensionIdExpr(dim))
      case LowerExpr(DimensionExpr(dim)) => Right(DimensionIdExpr(dim))
      case _                             => Left("Function id is applicable only to dimensions")
    }
  }
}
