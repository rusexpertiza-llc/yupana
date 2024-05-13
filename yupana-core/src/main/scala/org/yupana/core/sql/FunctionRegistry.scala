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
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType }
import org.yupana.core.ConstantCalculator

sealed trait TypeMatcher {
//  def matching(t: DataType): Boolean
}

case class ExactMatcher(dt: DataType) extends TypeMatcher {
//  override def matching(t: DataType): Boolean = dt == t
}

case object NumberMatcher extends TypeMatcher {
//  override def matching(t: DataType): Boolean = t.numeric.isDefined
}

case object OrdMatcher extends TypeMatcher {
//  override def matching(t: DataType): Boolean = t.ordering.isDefined
}

case class ArrowMatcher(from: TypeMatcher, to: TypeMatcher) extends TypeMatcher

//sealed trait TypeDeducer {
//  def deduce(resultType: DataType, resultMatcher: TypeMatcher): Option[DataType]
//}
//
//case class StrictDeducer(t: DataType) extends TypeDeducer {
//  override def deduce(resultType: DataType, resultMatcher: TypeMatcher): Option[DataType] = Some(t)
//}

object FunctionRegistry {

  type ArrayExpr[T] = Expression[Seq[T]]

  sealed trait ParamType
  case object NumberParam extends ParamType
  case object OrdParam extends ParamType
  case object AnyParam extends ParamType
  case object ArrayParam extends ParamType
  case class DataTypeParam(t: DataType) extends ParamType
  case object OtherParam extends ParamType

  case class FunctionDesc(
      name: String,
      t: ParamType,
      f: (ConstantCalculator, Expression[_]) => Either[String, Expression[_]]
  )
  case class Function2Desc(
      name: String,
      f: (ConstantCalculator, Expression[_], Expression[_]) => Either[String, Expression[_]]
  )

  trait Bind[A[_], Z] {
    def apply[T](a: A[T]): Z
  }

  trait Bind2[A[_], B[_], Z] {
    def apply[T](a: A[T], b: B[T]): Z
  }

  trait Bind2R[A[_], B[_], Z[_]] {
    def apply[T](a: A[T], b: B[T]): Z[T]
  }

  trait Bind3[A[_], B[_], C[_], Z] {
    def apply[T](a: A[T], b: B[T], c: C[T]): Z
  }

  val nullaryFunctions: Map[String, Expression[_]] = Map(
    "now" -> NowExpr
  )

  private val unaryFunctions: List[FunctionDesc] = List(
    // AGGREGATES
    FunctionDesc(
      "sum",
      NumberParam,
      (_, e: Expression[_]) => {
        e.dataType.meta.sqlTypeName match {
          case "TINYINT"  => Right(SumExpr[Byte, Int](e.asInstanceOf[Expression[Byte]]))
          case "SMALLINT" => Right(SumExpr[Short, Int](e.asInstanceOf[Expression[Short]]))
          case "INTEGER"  => Right(SumExpr[Int, Int](e.asInstanceOf[Expression[Int]]))
          case "BIGINT"   => Right(SumExpr[Long, Long](e.asInstanceOf[Expression[Long]]))
          case "DOUBLE"   => Right(SumExpr[Double, Double](e.asInstanceOf[Expression[Double]]))
          case "DECIMAL"  => Right(SumExpr[BigDecimal, BigDecimal](e.asInstanceOf[Expression[BigDecimal]]))
          case x          => Left(s"$x type is not available for Sum expression")
        }
      }
    ),
    uOrd(
      "min",
      new Bind2R[Expression, Ordering, Expression] {
        override def apply[T](e: Expression[T], o: Ordering[T]): Expression[T] = MinExpr(e)(o)
      }
    ),
    uOrd(
      "max",
      new Bind2R[Expression, Ordering, Expression] {
        override def apply[T](e: Expression[T], o: Ordering[T]): Expression[T] = MaxExpr(e)(o)
      }
    ),
    uAny("count", e => CountExpr(e)),
    uAny("distinct_count", e => DistinctCountExpr(e)),
    uAny("distinct_random", e => DistinctRandomExpr(e)),
    // WINDOW
    uAny("lag", e => LagExpr(e)),
    // REAL UNARY
    uNum(
      "-",
      new Bind2R[Expression, Numeric, Expression] {
        override def apply[T](e: Expression[T], n: Numeric[T]): Expression[T] = UnaryMinusExpr(e)(n)
      }
    ),
    uNum(
      "abs",
      new Bind2R[Expression, Numeric, Expression] {
        override def apply[T](e: Expression[T], n: Numeric[T]): Expression[T] = AbsExpr(e)(n)
      }
    ),
    uNum(
      "avg",
      new Bind2[Expression, Numeric, Expression[BigDecimal]] {
        override def apply[T](e: Expression[T], n: Numeric[T]): Expression[BigDecimal] = AvgExpr(e)(n)
      }
    ),
    uTyped("year", TruncYearExpr),
    uTyped("trunc_year", TruncYearExpr),
    uTyped("quarter", TruncQuarterExpr),
    uTyped("trunc_quarter", TruncQuarterExpr),
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
    uTyped("extract_quarter", ExtractQuarterExpr),
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
        override def apply[T](a: ArrayExpr[T]): Expression[_] = ArrayToStringExpr(a)
      }
    ),
    uArray(
      "length",
      new Bind[ArrayExpr, Expression[_]] {
        override def apply[T](a: ArrayExpr[T]): Expression[_] = ArrayLengthExpr(a)
      }
    ),
    uTyped("tokens", ArrayTokensExpr),
    // SPECIAL
    FunctionDesc("id", OtherParam, (_, e) => createDimIdExpr(e))
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
      (calculator: ConstantCalculator, a: Expression[_], b: Expression[_]) =>
        DataTypeUtils.alignTypes(a, b, calculator) match {
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
    biArrayAndElem(
      "contains",
      new Bind2[ArrayExpr, Expression, Expression[_]] {
        override def apply[T](a: ArrayExpr[T], b: Expression[T]): Expression[Boolean] = ContainsExpr(a, b)
      }
    ),
    biArray(
      "contains_any",
      new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
        override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsAnyExpr(a, b)
      }
    ),
    biArray(
      "contains_all",
      new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
        override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsAllExpr(a, b)
      }
    ),
    biArray(
      "contains_same",
      new Bind2[ArrayExpr, ArrayExpr, Expression[_]] {
        override def apply[T](a: ArrayExpr[T], b: ArrayExpr[T]): Expression[_] = ContainsSameExpr(a, b)
      }
    ),
    Function2Desc(
      "hll_count",
      (_, a: Expression[_], c: Expression[_]) =>
        c match {
          case ConstantExpr(v, _) =>
            val tpe = a.dataType.meta.sqlTypeName
            val std_err = v.asInstanceOf[BigDecimal]
            if (!Set("VARCHAR", "BIGINT", "SHORT", "TIMESTAMP").contains(tpe)) {
              Left("hll_count is not defined for given datatype: " + tpe)
            } else if (std_err < 0.00003 || std_err > 0.367) {
              Left("std_err must be in range (0.00003, 0.367), but: std_err=" + std_err)
            } else {
              c.dataType.numeric
                .map(n => HLLCountExpr(a, n.toDouble(v.asInstanceOf[c.dataType.T])))
                .toRight(s"$c must be a number")
            }

          case _ => Left(s"Expected constant but got $c")
        }
    )
  )

  def nullary(name: String): Either[String, Expression[_]] = {
    nullaryFunctions.get(name.toLowerCase).toRight(s"Undefined function $name")
  }

  def unary(
      name: String,
      calculator: ConstantCalculator,
      e: Expression[_]
  ): Either[String, Expression[_]] = {
    unaryFunctions.filter(_.name == name.toLowerCase) match {
      case Nil => Left(s"Undefined function $name")
      case xs =>
        val (r, l) = xs.map(_.f(calculator, e)).partition(_.isRight)
        if (r.isEmpty) {
          l.head
        } else if (r.size > 1) {
          Left(s"Ambiguous function '$name' call on $e")
        } else {
          r.head
        }
    }
  }

  def bi(
      name: String,
      calculator: ConstantCalculator,
      a: Expression[_],
      b: Expression[_]
  ): Either[String, Expression[_]] = {
    binaryFunctions.filter(_.name == name.toLowerCase) match {
      case Nil => Left(s"Undefined function $name")
      case xs =>
        val (r, l) = xs.map(_.f(calculator, a, b)).partition(_.isRight)
        if (r.isEmpty) {
          l.head
        } else if (r.size > 1) {
          Left(s"Ambiguous function '$name' call on ($a, $b)")
        } else {
          r.head
        }
    }
  }

  def functionsForType(t: DataType): Seq[String] = {
    val num = if (t.numeric.isDefined) unaryFunctions.filter(_.t == NumberParam) else Seq.empty
    val ord = if (t.ordering.isDefined) unaryFunctions.filter(_.t == OrdParam) else Seq.empty
    val tpe = unaryFunctions.filter(_.t == DataTypeParam(t))
    val array = if (t.kind == TypeKind.Array) unaryFunctions.filter(_.t == ArrayParam) else Seq.empty
    val any = unaryFunctions.filter(_.t == AnyParam)

    (num ++ ord ++ tpe ++ array ++ any).map(_.name).distinct.sorted
  }

  private def uNum(
      fn: String,
      create: Bind2R[Expression, Numeric, Expression]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      NumberParam,
      {
        case (_: ConstantCalculator, e: Expression[t]) =>
          e.dataType.numeric.fold[Either[String, Expression[t]]](Left(s"$fn requires a number, but got ${e.dataType}"))(
            num => Right(create(e, num))
          )
      }
    )
  }

  private def uNum[T](
      fn: String,
      create: Bind2[Expression, Numeric, Expression[T]]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      NumberParam,
      {
        case (_, e: Expression[t]) =>
          e.dataType.numeric.fold[Either[String, Expression[T]]](Left(s"$fn requires a number, but got ${e.dataType}"))(
            num => Right(create(e, num))
          )
      }
    )
  }

  private def uOrd(
      fn: String,
      create: Bind2R[Expression, Ordering, Expression]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      OrdParam,
      {
        case (_, e: Expression[t]) =>
          e.dataType.ordering.fold[Either[String, Expression[t]]](Left(s"$fn cannot be applied to ${e.dataType}"))(
            ord => Right(create(e, ord))
          )
      }
    )
  }

  private def uAny(fn: String, create: Expression[_] => Expression[_]): FunctionDesc =
    FunctionDesc(fn, AnyParam, (_, e) => Right(create(e)))

  private def uTyped[T](fn: String, create: Expression[T] => Expression[_])(
      implicit dt: DataType.Aux[T]
  ): FunctionDesc = {
    FunctionDesc(
      fn,
      DataTypeParam(dt),
      (_, e) =>
        if (e.dataType == dt) Right(create(e.asInstanceOf[Expression[T]]))
        else Left(s"Function $fn cannot be applied to $e of type ${e.dataType}")
    )
  }

  private def uArray[T](fn: String, create: Bind[ArrayExpr, Expression[_]]): FunctionDesc = {
    FunctionDesc(
      fn,
      ArrayParam,
      (_, e) =>
        if (e.dataType.kind == TypeKind.Array) Right(create(e.asInstanceOf[ArrayExpr[T]]))
        else Left(s"Function $fn requires array, but got $e")
    )
  }

  private def biOrd(
      fn: String,
      create: Bind3[Expression, Expression, Ordering, Expression.Condition]
  ): Function2Desc = {
    Function2Desc(
      fn,
      (calculator, a, b) =>
        DataTypeUtils.alignTypes(a, b, calculator) match {
          case Right(pair) if pair.dataType.ordering.isDefined =>
            Right(create(pair.a, pair.b, pair.dataType.ordering.get))
          case Right(_) => Left(s"Cannot compare types ${a.dataType} and ${b.dataType}")
          case Left(_)  => Left(s"Function $fn cannot be applied to $a, $b of types ${a.dataType}, ${b.dataType}")
        }
    )
  }

  private def biNum(
      fn: String,
      create: Bind3[Expression, Expression, Numeric, Expression[_]]
  ): Function2Desc = {
    Function2Desc(
      fn,
      (calculator, a, b) =>
        DataTypeUtils.alignTypes(a, b, calculator) match {
          case Right(pair) if pair.dataType.numeric.isDefined =>
            Right(create(pair.a, pair.b, pair.dataType.numeric.get))
          case Right(_) => Left(s"Cannot apply $fn to ${a.dataType} and ${b.dataType}")
          case Left(_)  => Left(s"Function $fn cannot be applied to $a, $b of types ${a.dataType}, ${b.dataType}")
        }
    )
  }

  private def biSame(fn: String, create: Bind2[Expression, Expression, Expression[_]]): Function2Desc = {
    Function2Desc(
      fn,
      (calculator, a, b) => DataTypeUtils.alignTypes(a, b, calculator).map(pair => create(pair.a, pair.b))
    )
  }

  private def biTyped[T, U](fn: String, create: (Expression[T], Expression[U]) => Expression[_])(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): Function2Desc = Function2Desc(
    fn,
    (_, a, b) =>
      if (a.dataType == dtt && b.dataType == dtu)
        Right(create(a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[U]]))
      else Left(s"Function $fn cannot be applied to $a, $b of types ${a.dataType}, ${b.dataType}")
  )

  private def biArray[T](fn: String, create: Bind2[ArrayExpr, ArrayExpr, Expression[_]]): Function2Desc = {
    Function2Desc(
      fn,
      (_, a, b) =>
        if (a.dataType.kind == TypeKind.Array && a.dataType == b.dataType)
          Right(create(a.asInstanceOf[ArrayExpr[T]], b.asInstanceOf[ArrayExpr[T]]))
        else Left(s"Function $fn requires two arrays of same type, but got $a, $b")
    )
  }

  private def biArrayAndElem[T](fn: String, create: Bind2[ArrayExpr, Expression, Expression[_]]): Function2Desc = {
    Function2Desc(
      fn,
      (_, a, b) =>
        if (a.dataType.kind == TypeKind.Array) {
          val adt = a.dataType.asInstanceOf[ArrayDataType[T]]
          if (adt.valueType == b.dataType) {
            Right(create(a.asInstanceOf[ArrayExpr[T]], b.asInstanceOf[Expression[T]]))
          } else Left(s"Function $fn second parameter should have type ${adt.valueType}")
        } else Left(s"Function $fn requires array and its element, but got $a and $b")
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
