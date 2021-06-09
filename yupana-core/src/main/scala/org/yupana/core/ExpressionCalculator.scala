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

package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType, TupleDataType }
import org.yupana.api.utils.Tokenizer
import org.yupana.core.model.InternalRow

import scala.collection.AbstractIterator

trait ExpressionCalculator extends Serializable {
  def evaluateFilter(tokenizer: Tokenizer, internalRow: InternalRow): Boolean
  def evaluateExpressions(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateReduce(tokenizer: Tokenizer, a: InternalRow, b: InternalRow): InternalRow
  def evaluatePostMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostAggregateExprs(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostFilter(tokenizer: Tokenizer, row: InternalRow): Boolean

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O = {
    winFuncExpr match {
      case LagExpr(_) => if (index > 0) values(index - 1).asInstanceOf[O] else null.asInstanceOf[O]
    }
  }
}

object ExpressionCalculator extends StrictLogging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox

  private val tokenizer = TermName("tokenizer")

  private def className(dataType: DataType): String = {
    val tpe = dataType.classTag.toString()
    val lastDot = tpe.lastIndexOf(".")
    tpe.substring(lastDot + 1)
  }

  private def isNullable(e: Expression[_]): Boolean = {
    e match {
      case ConstantExpr(_)  => false
      case TimeExpr         => false
      case DimensionExpr(_) => false
      case _                => true
    }
  }

  private def mkType(dataType: DataType): Tree = {
    dataType.kind match {
      case TypeKind.Regular => Ident(TypeName(className(dataType)))
      case TypeKind.Tuple =>
        val tt = dataType.asInstanceOf[TupleDataType[_, _]]
        AppliedTypeTree(
          Ident(TypeName("Tuple2")),
          List(Ident(TypeName(className(tt.aType))), Ident(TypeName(className(tt.bType))))
        )
      case TypeKind.Array =>
        val at = dataType.asInstanceOf[ArrayDataType[_]]
        AppliedTypeTree(
          Ident(TypeName("Seq")),
          List(Ident(TypeName(className(at.valueType))))
        )
    }
  }

  private def mkType(e: Expression[_]): Tree = {
    mkType(e.dataType)
  }

  private def mapValue(tpe: DataType)(v: Any): Tree = {
    import scala.reflect.classTag

    tpe.kind match {
      case TypeKind.Regular if tpe.classTag == classTag[Time] =>
        val t = v.asInstanceOf[Time]
        q"_root_.org.yupana.api.Time(${t.millis})"

      case TypeKind.Regular if tpe.classTag == classTag[BigDecimal] =>
        val str = v.asInstanceOf[BigDecimal].toString()
        q"""_root_.scala.math.BigDecimal($str)"""

      case TypeKind.Regular => Literal(Constant(v))

      case TypeKind.Tuple =>
        val tt = tpe.asInstanceOf[TupleDataType[_, _]]
        val (a, b) = v.asInstanceOf[(_, _)]
        Apply(Ident(TermName("Tuple2")), List(mapValue(tt.aType)(a), mapValue(tt.bType)(b)))

      case TypeKind.Array => throw new IllegalAccessException("Didn't expect to see Array here")
    }
  }

  private def mkGet(known: Map[Expression[_], Int], row: TermName, e: Expression[_]): Tree = {
    val tpe = mkType(e)
    e match {
      case ConstantExpr(x) =>
        val lit = mapValue(e.dataType)(x)
        q"$lit.asInstanceOf[$tpe]"
      case ae @ ArrayExpr(exprs) if e.kind == Const =>
        val lits = exprs.map {
          case ConstantExpr(v) => mapValue(ae.elementDataType)(v)
          case x               => throw new IllegalArgumentException(s"Unexpected constant expression $x")
        }
        val innerTpe = mkType(ae.elementDataType)
        q"Seq[$innerTpe](..$lits)"
      case x =>
        val idx = known(x)
        q"$row.get[$tpe]($idx)"
    }
  }

  private def mkIsDefined(known: Map[Expression[_], Int], row: TermName, e: Expression[_]): Tree = {
    val idx = known(e)
    q"$row.isDefined($idx)"
  }

  private def mkSetUnary(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_],
      a: Expression[_],
      f: Tree => Tree,
      elseTree: Option[Tree] = None
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    val (prepare, newKnown) = mkSet(row, known, onlySimple, a)
    val set = newKnown.get(e).map { idx =>
      val getA = mkGet(newKnown, row, a)
      val tpe = mkType(e)
      val v = f(getA)
      val aIsNullable = isNullable(a)
      if (aIsNullable) {
        val aIsDefined = mkIsDefined(newKnown, row, a)
        elseTree match {
          case Some(e) =>
            q"$row.set($idx, if ($aIsDefined) $v.asInstanceOf[$tpe] else $e.asInstanceOf[$tpe])"

          case None =>
            q"if ($aIsDefined) $row.set($idx, $v.asInstanceOf[$tpe])"
        }
      } else {
        q"$row.set($idx, $v.asInstanceOf[$tpe])"
      }
    }
    (prepare ++ set.toSeq, newKnown)
  }

  def mkSetMathUnary(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_],
      a: Expression[_],
      fun: TermName
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    if (a.dataType.integral.nonEmpty) {
      mkSetUnary(row, known, onlySimple, e, a, x => q"${integralValName(a.dataType)}.$fun($x)")
    } else {
      mkSetUnary(row, known, onlySimple, e, a, x => q"${fractionalValName(a.dataType)}.$fun($x)")
    }
  }

  private def mkSetTypeConvertExpr(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_],
      a: Expression[_]
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    val mapper = typeConverters.getOrElse(
      (a.dataType.meta.sqlTypeName, e.dataType.meta.sqlTypeName),
      throw new IllegalArgumentException(s"Unsupported type conversion ${a.dataType} to ${e.dataType}")
    )
    mkSetUnary(row, known, onlySimple, e, a, mapper)
  }

  private def mkSetBinary(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_],
      a: Expression[_],
      b: Expression[_],
      f: (Tree, Tree) => Tree
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    val (prepare, newKnown) = mkSetExprs(row, known, onlySimple, Seq(a, b))
    val set = newKnown.get(e).map { idx =>
      val getA = mkGet(newKnown, row, a)
      val getB = mkGet(newKnown, row, b)
      val tpe = mkType(e)
      val v = f(getA, getB)

      val aIsNullable = isNullable(a)
      val bIsNullable = isNullable(b)

      val aDefined = mkIsDefined(newKnown, row, a)
      val bDefined = mkIsDefined(newKnown, row, b)

      if (aIsNullable && bIsNullable) {
        q"if ($aDefined && $bDefined) $row.set($idx, $v.asInstanceOf[$tpe])"
      } else if (aIsNullable) {
        q"if ($aDefined) $row.set($idx, $v.asInstanceOf[$tpe])"
      } else if (bIsNullable) {
        q"if ($bDefined) $row.set($idx, $v.asInstanceOf[$tpe])"
      } else {
        q"$row.set($idx, $v.asInstanceOf[$tpe])"
      }
    }
    (prepare ++ set.toSeq, newKnown)
  }

  private def tcEntry[A, B](
      aToB: Tree => Tree
  )(implicit a: DataType.Aux[A], b: DataType.Aux[B]): ((String, String), Tree => Tree) = {
    ((a.meta.sqlTypeName, b.meta.sqlTypeName), aToB)
  }

  private val typeConverters: Map[(String, String), Tree => Tree] = Map(
    tcEntry[Double, BigDecimal](d => q"BigDecimal($d)"),
    tcEntry[Long, BigDecimal](l => q"BigDecimal($l)"),
    tcEntry[Long, Double](l => q"$l.toDouble"),
    tcEntry[Int, Long](i => q"$i.toLong"),
    tcEntry[Int, BigDecimal](i => q"BigDecimal($i)"),
    tcEntry[Short, BigDecimal](s => q"BigDecimal($s)"),
    tcEntry[Byte, BigDecimal](b => q"BigDecimal($b)")
  )

  private val truncTime = q"_root_.org.yupana.core.ExpressionCalculator.truncateTime"
  private val dtft = q"_root_.org.joda.time.DateTimeFieldType"

  private def exprValName(e: Expression[_]): TermName = {
    TermName(s"e_${e.hashCode()}")
  }

  private def ordValName(dt: DataType): TermName = {
    TermName(s"ord_${dt.toString}")
  }

  private def fractionalValName(dt: DataType): TermName = {
    TermName(s"frac_${dt.toString}")
  }

  private def integralValName(dt: DataType): TermName = {
    TermName(s"int_${dt.toString}")
  }

  private def mkLogical(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_],
      cs: Seq[Condition],
      reducer: Tree
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    val (sets, newKnown) = mkSetExprs(row, known, onlySimple, cs)
    val tree = newKnown.get(e).map { idx =>
      val gets = cs.map(c => mkGet(newKnown, row, c))
      q"""
        val vs = List(..$gets)
        val res = vs.reduce($reducer)
        $row.set($idx, res)
      """
    }
    (sets ++ tree.toSeq, newKnown)
  }

  private def mkSet(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      e: Expression[_]
  ): (Seq[Tree], Map[Expression[_], Int]) = {

    if (known.contains(e)) {
      (Nil, known)
    } else {
      val newKnown =
        if (!onlySimple ||
            e.kind == Simple ||
            e.kind == Const ||
            e.isInstanceOf[AggregateExpr[_, _, _]] ||
            e.isInstanceOf[WindowFunctionExpr[_, _]]) known + (e -> known.size)
        else known
      e match {
        case ConstantExpr(c) =>
          val v = mapValue(e.dataType)(c)
          newKnown.get(e).map(idx => Seq(q"$row.set($idx, $v)") -> newKnown).getOrElse((Nil, known))
        case TimeExpr             => (Nil, newKnown)
        case DimensionExpr(_)     => (Nil, newKnown)
        case DimensionIdExpr(_)   => (Nil, newKnown)
        case MetricExpr(_)        => (Nil, newKnown)
        case DimIdInExpr(_, _)    => (Nil, newKnown)
        case DimIdNotInExpr(_, _) => (Nil, newKnown)
        case LinkExpr(link, _) =>
          val dimExpr = DimensionExpr(link.dimension)
          if (newKnown.contains(dimExpr)) (Nil, newKnown)
          else (Nil, newKnown + (dimExpr -> newKnown.size))

        case _: AggregateExpr[_, _, _] => (Nil, newKnown)
        case we: WindowFunctionExpr[_, _] =>
          val (tree1, k1) = mkSet(row, newKnown, onlySimple, TimeExpr)
          val (tree2, newestKnown) = mkSet(row, k1, onlySimple, we.expr)
          (tree1 ++ tree2, newestKnown)

        case TupleExpr(a, b) => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"($x, $y)")

        case GtExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x > $y""")
        case LtExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x < $y""")
        case GeExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x >= $y""")
        case LeExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x <= $y""")
        case EqExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x == $y""")
        case NeqExpr(a, b) => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x != $y""")

        case InExpr(v, _) => mkSetUnary(row, newKnown, onlySimple, e, v, x => q"""${exprValName(e)}.contains($x)""")
        case NotInExpr(v, _) =>
          mkSetUnary(row, newKnown, onlySimple, e, v, x => q"""!${exprValName(e)}.contains($x)""")

        case PlusExpr(a, b)    => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x + $y""")
        case MinusExpr(a, b)   => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x - $y""")
        case TimesExpr(a, b)   => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x * $y""")
        case DivIntExpr(a, b)  => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x / $y""")
        case DivFracExpr(a, b) => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"""$x / $y""")

        case TypeConvertExpr(_, a) => mkSetTypeConvertExpr(row, newKnown, onlySimple, e, a)

        case TruncYearExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.year())($x)""")
        case TruncMonthExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.monthOfYear())($x)""")
        case TruncWeekExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.weekOfWeekyear())($x)""")
        case TruncDayExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.dayOfMonth())($x)""")
        case TruncHourExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.hourOfDay())($x)""")
        case TruncMinuteExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.minuteOfHour())($x)""")
        case TruncSecondExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$truncTime($dtft.secondOfMinute())($x)""")
        case ExtractYearExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getYear")
        case ExtractMonthExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getMonthOfYear")
        case ExtractDayExpr(a)  => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getDayOfMonth")
        case ExtractHourExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getHourOfDay")
        case ExtractMinuteExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getMinuteOfHour")
        case ExtractSecondExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLocalDateTime.getSecondOfMinute")

        case TimeMinusExpr(a, b) =>
          mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)")
        case TimeMinusPeriodExpr(a, b) =>
          mkSetBinary(row, newKnown, onlySimple, e, a, b, (t, p) => q"Time($t.toDateTime.minus($p).getMillis)")
        case TimePlusPeriodExpr(a, b) =>
          mkSetBinary(row, newKnown, onlySimple, e, a, b, (t, p) => q"Time($t.toDateTime.plus($p).getMillis)")
        case PeriodPlusPeriodExpr(a, b) => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"$x plus $y")

        case IsNullExpr(a)    => mkSetUnary(row, newKnown, onlySimple, e, a, _ => q"false", Some(q"true"))
        case IsNotNullExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, _ => q"true", Some(q"false"))

        case LowerExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toLowerCase")
        case UpperExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.toUpperCase")

        case ConditionExpr(c, p, n) =>
          val (prepare, newestKnown) = mkSetExprs(row, newKnown, onlySimple, Seq(c, p, n))
          val set = newestKnown.get(e).map { idx =>
            val getC = mkGet(newestKnown, row, c)
            val getP = mkGet(newestKnown, row, p)
            val getN = mkGet(newestKnown, row, n)
            q"$row.set($idx, if ($getC) $getP else $getN)"
          }
          (prepare ++ set.toSeq, newestKnown)

        case AbsExpr(a)        => mkSetMathUnary(row, newKnown, onlySimple, e, a, TermName("abs"))
        case UnaryMinusExpr(a) => mkSetMathUnary(row, newKnown, onlySimple, e, a, TermName("negate"))

        case NotExpr(a)  => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"!$x")
        case AndExpr(cs) => mkLogical(row, newKnown, onlySimple, e, cs, q"_ && _")
        case OrExpr(cs)  => mkLogical(row, newKnown, onlySimple, e, cs, q"_ || _")

        case TokensExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$tokenizer.transliteratedTokens($x)")
        case SplitExpr(a) =>
          mkSetUnary(
            row,
            newKnown,
            onlySimple,
            e,
            a,
            x => q"_root_.org.yupana.core.ExpressionCalculator.splitBy($x, !_.isLetterOrDigit).toSeq"
          )
        case LengthExpr(a)    => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.length")
        case ConcatExpr(a, b) => mkSetBinary(row, newKnown, onlySimple, e, a, b, (x, y) => q"$x + $y")

        case ArrayExpr(exprs) =>
          val (sets, newestKnown) = mkSetExprs(row, newKnown, onlySimple, exprs)
          val tree = newestKnown.get(e).map { idx =>
            val gets = exprs.map(a => mkGet(newestKnown, row, a))
            q"$row.set($idx, Seq(..$gets))"
          }
          (sets ++ tree.toSeq, newestKnown)

        case ArrayLengthExpr(a)   => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"$x.size")
        case ArrayToStringExpr(a) => mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$x.mkString(", ")""")
        case ArrayTokensExpr(a) =>
          mkSetUnary(row, newKnown, onlySimple, e, a, x => q"""$x.flatMap(s => $tokenizer.transliteratedTokens(s))""")

        case ContainsExpr(as, b) => mkSetBinary(row, newKnown, onlySimple, e, as, b, (x, y) => q"$x.contains($y)")
        case ContainsAnyExpr(as, bs) =>
          mkSetBinary(row, newKnown, onlySimple, e, as, bs, (x, y) => q"$y.exists($x.contains)")
        case ContainsAllExpr(as, bs) =>
          mkSetBinary(row, newKnown, onlySimple, e, as, bs, (x, y) => q"$y.forall($x.contains)")
        case ContainsSameExpr(as, bs) =>
          mkSetBinary(row, newKnown, onlySimple, e, as, bs, (x, y) => q"$x.size == $y.size && $x.toSet == $y.toSet")
      }
    }
  }

  private def mkFilter(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      condition: Option[Expression.Condition]
  ): (Tree, Map[Expression[_], Int]) = {
    condition match {
      case Some(ConstantExpr(v)) => (q"$v", known)

      case Some(cond) =>
        val (set, newKnown) = mkSet(row, known, onlySimple, cond)
        val get = mkGet(newKnown, row, cond)
        val tree = q"""
           ..$set
           $get
         """
        (tree, newKnown)

      case None => (q"true", known)
    }
  }

  private def mkSetExprs(
      row: TermName,
      known: Map[Expression[_], Int],
      onlySimple: Boolean,
      exprs: Seq[Expression[_]]
  ): (Seq[Tree], Map[Expression[_], Int]) = {
    exprs.foldLeft((Seq.empty[Tree], known)) {
      case ((ss, k), c) =>
        val (t, nk) = mkSet(row, k, onlySimple, c)
        (ss ++ t, nk)
    }
  }

  private def mkEvaluate(
      query: Query,
      row: TermName,
      known: Map[Expression[_], Int]
  ): (Tree, Map[Expression[_], Int]) = {
    val (trees, newKnown) =
      mkSetExprs(row, known, onlySimple = true, query.fields.map(_.expr).toList ++ query.groupBy)
    (q"..$trees", newKnown)
  }

  private def mkMap(
      known: Map[Expression[_], Int],
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): (Tree, Map[Expression[_], Int]) = {
    val (prepare, newKnown) = mkSetExprs(row, known, onlySimple = false, aggregates.map(_.expr))

    val trees = aggregates.map { ae =>

      val idx = newKnown(ae)

      val exprValue = mkGet(newKnown, row, ae.expr)

      val value = ae match {
        case SumExpr(_)            => exprValue
        case MinExpr(_)            => exprValue
        case MaxExpr(_)            => exprValue
        case CountExpr(_)          => q"if ($exprValue != null) 1L else 0L"
        case DistinctCountExpr(_)  => q"if ($exprValue != null) Set($exprValue) else Set.empty"
        case DistinctRandomExpr(_) => q"if ($exprValue != null) Set($exprValue) else Set.empty"
      }
      q"$row.set($idx, $value)"
    }

    val tree = q"""
      ..$prepare
      ..$trees
      """
    (tree, newKnown)
  }

  private def mkReduce(
      known: Map[Expression[_], Int],
      aggregates: Seq[AggregateExpr[_, _, _]],
      rowA: TermName,
      rowB: TermName,
      outRow: TermName
  ): Tree = {
    val trees = aggregates.map { ae =>
      val idx = known(ae)
      val valueTpe = mkType(ae.expr)

      val aValue = mkGet(known, rowA, ae)
      val bValue = mkGet(known, rowB, ae)

      val value = ae match {
        case SumExpr(_)            => q"$aValue + $bValue"
        case MinExpr(_)            => q"${ordValName(ae.expr.dataType)}.min($aValue, $bValue)"
        case MaxExpr(_)            => q"${ordValName(ae.expr.dataType)}.max($aValue, $bValue)"
        case CountExpr(_)          => q"$rowA.get[Long]($idx) + $rowB.get[Long]($idx)"
        case DistinctCountExpr(_)  => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
        case DistinctRandomExpr(_) => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
      }
      q"$outRow.set($idx, $value)"
    }

    q"..$trees"
  }

  private def mkPostMap(
      known: Map[Expression[_], Int],
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): Tree = {
    val trees = aggregates.flatMap { ae =>
      val idx = known(ae)
      val valueTpe = mkType(ae.expr)

      val oldValue = mkGet(known, row, ae)

      val value = ae match {
        case SumExpr(_)           => Some(q"if ($oldValue != null) $oldValue else 0")
        case MinExpr(_)           => None
        case MaxExpr(_)           => None
        case CountExpr(_)         => None
        case DistinctCountExpr(_) => Some(q"$row.get[Set[$valueTpe]]($idx).size")
        case DistinctRandomExpr(_) =>
          Some(
            q"""
              val s = $row.get[Set[$valueTpe]]($idx)
              val n = _root_.scala.util.Random.nextInt(s.size)
              s.iterator.drop(n).next
            """
          )
      }

      value.map(v => q"$row.set($idx, $v)")
    }

    q"..$trees"
  }

  private def mkPostAggregate(
      query: Query,
      row: TermName,
      known: Map[Expression[_], Int]
  ): (Tree, Map[Expression[_], Int]) = {
    val (trees, nk) = mkSetExprs(row, known, onlySimple = false, query.fields.map(_.expr))

    (q"..$trees", nk)
  }

  private def mkVars(known: Map[Expression[_], Int]): Seq[Tree] = {
    def setVariable[T: TypeTag](e: Expression[_], inner: Expression[_], values: Set[T]): Tree = {

      val literals = values.toList.map(mapValue(inner.dataType))
      val v = Apply(Ident(TermName("Set")), literals)
      val tpe = mkType(inner)

      q"private val ${exprValName(e)}: Set[$tpe] = $v"
    }

    val inVars = known.keys.collect {
      case e @ InExpr(i, values)    => setVariable(e, i, values)
      case e @ NotInExpr(i, values) => setVariable(e, i, values)
    }.toSeq

    val dtVars = known.keySet.map(_.dataType).toList.flatMap { dt =>
      val tpe = mkType(dt)
      DataType
        .bySqlName(dt.meta.sqlTypeName)
        .toList
        .flatMap { _ =>
          Seq(
            dt.ordering.map(_ =>
              q"private val ${ordValName(dt)}: Ordering[$tpe] = DataType.bySqlName(${dt.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$tpe]].ordering.get"
            ),
            dt.fractional.map(_ =>
              q"private val ${fractionalValName(dt)}: Fractional[$tpe] = DataType.bySqlName(${dt.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$tpe]].fractional.get"
            ),
            dt.integral.map(_ =>
              q"private val ${integralValName(dt)}: Integral[$tpe] = DataType.bySqlName(${dt.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$tpe]].integral.get"
            )
          ).flatten
        }
    }

    inVars ++ dtVars
  }

  def generateCalculator(query: Query, condition: Option[Condition]): (Tree, Map[Expression[_], Int]) = {
    val internalRow = TermName("internalRow")
    val (filter, k1) = mkFilter(internalRow, Map.empty, onlySimple = true, condition)

    val (evaluate, k2) = mkEvaluate(query, internalRow, k1)

    val knownAggregates = k2.collect { case (ae: AggregateExpr[_, _, _], _) => ae }.toSeq

    val (map, k3) = mkMap(k2, knownAggregates, internalRow)

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val outRow = rowA
    val reduce = mkReduce(k3, knownAggregates, rowA, rowB, outRow)

    val postMap = mkPostMap(k3, knownAggregates, internalRow)
    val (postAggregate, k4) = mkPostAggregate(query, internalRow, k3)

    val (postFilter, k5) = mkFilter(internalRow, k4, onlySimple = false, query.postFilter)
    val defs = mkVars(k5)

    val tree = q"""
        import _root_.org.yupana.api.Time
        import _root_.org.yupana.api.types.DataType
        import _root_.org.yupana.api.utils.Tokenizer
        import _root_.org.yupana.core.model.InternalRow
        
        new _root_.org.yupana.core.ExpressionCalculator {
          ..$defs
        
          override def evaluateFilter($tokenizer: Tokenizer, $internalRow: InternalRow): Boolean = $filter
          
          override def evaluateExpressions($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $evaluate
            $internalRow
          }
          
          override def evaluateMap($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $map
            $internalRow
          }
          
          override def evaluateReduce($tokenizer: Tokenizer, $rowA: InternalRow, $rowB: InternalRow): InternalRow = {
            $reduce
            $outRow
          }
    
          override def evaluatePostMap($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $postMap
            $internalRow
          }
    
          override def evaluatePostAggregateExprs($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $postAggregate
            $internalRow
          }

          override def evaluatePostFilter($tokenizer: Tokenizer, $internalRow: InternalRow): Boolean = $postFilter
        }
    """

    tree -> k5
  }

  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int]) = {
    val tb = currentMirror.mkToolBox()

    val (tree, known) = generateCalculator(query, condition)

    logger.whenTraceEnabled {
      val index = known.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      index.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tb.compile(tree)().asInstanceOf[ExpressionCalculator], known)
  }

  private def prettyTree(tree: Tree): String = {
    show(tree)
      .replaceAll("_root_\\.([a-z_]+\\.)+", "")
      .replaceAll("\\.\\$bang\\$eq", " != ")
      .replaceAll("\\.\\$eq\\$eq", " == ")
      .replaceAll("\\.\\$amp\\$amp", " && ")
      .replaceAll("\\.\\$plus\\$plus", " ++ ")
      .replaceAll("\\.\\$plus", " + ")
      .replaceAll("\\.\\$minus", " - ")
      .replaceAll("\\.\\$div", " / ")
  }

  def truncateTime(fieldType: DateTimeFieldType)(time: Time): Time = {
    Time(time.toDateTime.property(fieldType).roundFloorCopy().getMillis)
  }

  def splitBy(s: String, p: Char => Boolean): Iterator[String] = new AbstractIterator[String] {
    private val len = s.length
    private var pos = 0

    override def hasNext: Boolean = pos < len

    override def next(): String = {
      if (pos >= len) throw new NoSuchElementException("next on empty iterator")
      val start = pos
      while (pos < len && !p(s(pos))) pos += 1
      val res = s.substring(start, pos min len)
      while (pos < len && p(s(pos))) pos += 1
      res
    }
  }
}
