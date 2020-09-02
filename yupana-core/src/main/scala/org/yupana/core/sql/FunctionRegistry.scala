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

  type ArrayExpr[T] = Expression[Array[T]]

  sealed trait ParamType
  case object NumberParam extends ParamType
  case class DataTypeParam(t: DataType) extends ParamType
  case object OtherParam extends ParamType

  case class FunctionDesc(name: String, t: ParamType, f: Expression[_] => Either[String, Expression[_]])
  case class Function2Desc(name: String, f: (Expression[_], Expression[_]) => Either[String, Expression[_]])

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
    uNum("sum", new Bind2[Expression, Numeric, Expression[_]] {
      override def apply[T](e: Expression[T], n: Numeric[T]): Expression[_] = SumExpr(e)(n)
    }),
    uOrd("min", new Bind2[Expression, Ordering, Expression[_]] {
      override def apply[T](e: Expression[T], o: Ordering[T]): Expression[_] = MinExpr(e)(o)
    }),
    uOrd("max", new Bind2[Expression, Ordering, Expression[_]] {
      override def apply[T](e: Expression[T], o: Ordering[T]): Expression[_] = MaxExpr(e)(o)
    }),
    uAny("count", e => CountExpr(e)),
    uAny("distinct_count", e => DistinctCountExpr(e)),
    uAny("distinct_random", e => DistinctRandomExpr(e)),
    // WINDOW
    uAny("lag", e => LagExpr(e)),
    // REAL UNARY
    uNum("-", new Bind2[Expression, Numeric, Expression[_]] {
      override def apply[T](e: Expression[T], n: Numeric[T]): Expression[_] = UnaryMinusExpr(e)(n)
    }),
    uNum("abs", new Bind2[Expression, Numeric, Expression[_]] {
      override def apply[T](e: Expression[T], n: Numeric[T]): Expression[_] = AbsExpr(e)(n)
    }),
    uTyped("year", TruncYearExpr),
    uTyped("trunc_year", TruncYearExpr),
    uTyped("month", TruncMonthExpr),
    uTyped("trunc_month", TruncMonthExpr),
    uTyped("week", TruncWeekExpr),
    uTyped("trunc_week", TruncWeekExpr),
    uTyped("day", TruncDayExpr),
    uTyped("trunc_day", TruncDayExpr),
    uTyped("hour", TruncHourExpr),
    uTyped("trunc_hour", TruncHourExpr),
    uTyped("minute", TruncMinuteExpr),
    uTyped("trunc_minute", TruncMinuteExpr),
    uTyped("second", TruncSecondExpr),
    uTyped("trunc_second", TruncSecondExpr),
    uTyped("extract_year", ExtractYearExpr),
    uTyped("extract_month", ExtractMonthExpr),
    uTyped("extract_day", ExtractDayExpr),
    uTyped("extract_hour", ExtractHourExpr),
    uTyped("extract_minute", ExtractMinuteExpr),
    uTyped("extract_second", ExtractSecondExpr),
    uTyped("length", LengthExpr),
    uAny("is_null", e => IsNullExpr(e)),
    uAny("is_not_null", e => IsNotNullExpr(e)),
    uTyped("not", NotExpr),
    uTyped("tokens", TokensExpr),
    uTyped("split", SplitExpr),
    uTyped("lower", LowerExpr),
    uTyped("upper", UpperExpr),
    uArray(
      "array_to_string",
      new Bind[ArrayExpr, Expression[_]] {
        override def apply[T](a: Expression[Array[T]]): Expression[_] = ArrayToStringExpr(a)
      }
    ),
    uArray("length", new Bind[ArrayExpr, Expression[_]] {
      override def apply[T](a: Expression[Array[T]]): Expression[_] = ArrayLengthExpr(a)
    }),
    uTyped("tokens", ArrayTokensExpr),
    // SPECIAL
    FunctionDesc("id", OtherParam, createDimIdExpr)
  )

  private val binaryFunctions: List[Function2Desc] = List(
    biOrd(
      ">=",
      new Bind3[Expression, Expression, Ordering, Condition] {
        override def apply[T](a: Expression[T], b: Expression[T], o: Ordering[T]): Condition = GeExpr(a, b)(o)
      }
    ),
    biOrd(
      ">",
      new Bind3[Expression, Expression, Ordering, Condition] {
        override def apply[T](a: Expression[T], b: Expression[T], o: Ordering[T]): Condition = GtExpr(a, b)(o)
      }
    ),
    biOrd(
      "<=",
      new Bind3[Expression, Expression, Ordering, Condition] {
        override def apply[T](a: Expression[T], b: Expression[T], o: Ordering[T]): Condition = LeExpr(a, b)(o)
      }
    ),
    biOrd(
      "<",
      new Bind3[Expression, Expression, Ordering, Condition] {
        override def apply[T](a: Expression[T], b: Expression[T], o: Ordering[T]): Condition = LtExpr(a, b)(o)
      }
    ),
    biSame(
      "=",
      new Bind2[Expression, Expression, Expression[_]] {
        override def apply[T](a: Expression[T], b: Expression[T]): Condition = EqExpr(a, b)
      }
    ),
    biSame(
      "<>",
      new Bind2[Expression, Expression, Expression[_]] {
        override def apply[T](a: Expression[T], b: Expression[T]): Condition = NeqExpr(a, b)
      }
    ),
    biNum(
      "+",
      new Bind3[Expression, Expression, Numeric, Expression[_]] {
        override def apply[T](a: Expression[T], b: Expression[T], n: Numeric[T]): Expression[_] = PlusExpr(a, b)(n)
      }
    ),
    biNum(
      "-",
      new Bind3[Expression, Expression, Numeric, Expression[_]] {
        override def apply[T](a: Expression[T], b: Expression[T], n: Numeric[T]): Expression[_] =
          MinusExpr(a, b)(n)
      }
    ),
    biNum(
      "*",
      new Bind3[Expression, Expression, Numeric, Expression[_]] {
        override def apply[T](a: Expression[T], b: Expression[T], n: Numeric[T]): Expression[_] =
          TimesExpr(a, b)(n)
      }
    ),
    Function2Desc(
      "/",
      (a: Expression[_], b: Expression[_]) =>
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
    biArray("contains_any", new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsAnyExpr(a, b)
    }),
    biArray("contains_all", new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsAnyExpr(a, b)
    }),
    biArray("contains_same", new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
      override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsSameExpr(a, b)
    })
  )

  def unary(name: String, e: Expression[_]): Either[String, Expression[_]] = {
    unaryFunctions.filter(_.name == name.toLowerCase) match {
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

  def bi(name: String, a: Expression[_], b: Expression[_]): Either[String, Expression[_]] = {
    binaryFunctions.filter(_.name == name.toLowerCase) match {
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

  def functionsForType(t: DataType): Seq[String] = {
    val fs = if (t.numeric.isDefined) {
      unaryFunctions.filter(_.t == NumberParam)
    } else {
      unaryFunctions.filter(_.t == DataTypeParam(t))
    }

    fs.map(_.name)
  }

  private def uNum(
      fn: String,
      create: Bind2[Expression, Numeric, Expression[_]]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      NumberParam,
      e =>
        e.dataType.numeric match {
          case Some(num) => Right(create(e, num))
          case None      => Left(s"$fn requires a number, but got ${e.dataType}")
        }
    )
  }

  private def uOrd(
      fn: String,
      create: Bind2[Expression, Ordering, Expression[_]]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      OtherParam,
      e =>
        e.dataType.ordering match {
          case Some(ord) => Right(create(e, ord))
          case None      => Left(s"$fn cannot be applied to ${e.dataType}")
        }
    )
  }

  private def uAny(fn: String, create: Expression[_] => Expression[_]): FunctionDesc =
    FunctionDesc(fn, OtherParam, e => Right(create(e)))

  private def uTyped[T](fn: String, create: Expression[T] => Expression[_])(
      implicit dt: DataType.Aux[T]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      DataTypeParam(dt),
      e =>
        if (e.dataType == dt) Right(create(e.asInstanceOf[Expression[T]]))
        else Left(s"Function $fn cannot be applied to $e of type ${e.dataType}")
    )
  }

  private def uArray[T](fn: String, create: Bind[ArrayExpr, Expression[_]]): FunctionDesc = {
    FunctionDesc(
      fn,
      OtherParam,
      e =>
        if (e.dataType.isArray) Right(create(e.asInstanceOf[Expression[Array[T]]]))
        else Left(s"Function $fn requires array, but got $e")
    )
  }

  private def biOrd(
      fn: String,
      create: Bind3[Expression, Expression, Ordering, Expression.Condition]
  ): Function2Desc = {
    Function2Desc(
      fn,
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
      create: Bind3[Expression, Expression, Numeric, Expression[_]]
  ): Function2Desc = {
    Function2Desc(
      fn,
      (a, b) =>
        ExprPair.alignTypes(a, b) match {
          case Right(pair) if pair.dataType.numeric.isDefined =>
            Right(create(pair.a, pair.b, pair.dataType.numeric.get))
          case Right(_)  => Left(s"Cannot apply $fn to ${a.dataType} and ${b.dataType}")
          case Left(msg) => Left(msg)

        }
    )
  }

  private def biSame(fn: String, create: Bind2[Expression, Expression, Expression[_]]): Function2Desc = {
    Function2Desc(
      fn,
      (a, b) => ExprPair.alignTypes(a, b).right.map(pair => create(pair.a, pair.b))
    )
  }

  private def biTyped[T, U](fn: String, create: (Expression[T], Expression[U]) => Expression[_])(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): Function2Desc = Function2Desc(
    fn,
    (a, b) =>
      if (a.dataType == dtt && b.dataType == dtu)
        Right(create(a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[U]]))
      else Left(s"Function $fn cannot be applied to $a, $b of types ${a.dataType}, ${b.dataType}")
  )

  private def biArray[T](fn: String, create: Bind2[ArrayExpr, ArrayExpr, Expression[_]]): Function2Desc = {
    Function2Desc(
      fn,
      (a, b) =>
        if (a.dataType.isArray && b.dataType.isArray)
          Right(create(a.asInstanceOf[Expression[Array[T]]], b.asInstanceOf[Expression[Array[T]]]))
        else Left(s"Function $fn requires array, but got $a, $b")
    )
  }

  private def createDimIdExpr(expr: Expression[_]): Either[String, Expression[_]] = {
    expr match {
      case DimensionExpr(dim)            => Right(DimensionIdExpr(dim))
      case LowerExpr(DimensionExpr(dim)) => Right(DimensionIdExpr(dim))
      case _                             => Left("Function id is applicable only to dimensions")
    }
  }
}
