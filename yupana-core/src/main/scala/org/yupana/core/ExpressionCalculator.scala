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
import org.yupana.api.types.{ DataType, TupleDataType }
import org.yupana.core.model.InternalRow

import scala.collection.AbstractIterator

trait ExpressionCalculator {
  def evaluateFilter(row: InternalRow): Boolean
  def evaluateExpressions(internalRow: InternalRow): InternalRow
  def evaluateMap(internalRow: InternalRow): InternalRow
  def evaluateReduce(a: InternalRow, b: InternalRow): InternalRow
  def evaluatePostMap(internalRow: InternalRow): InternalRow
  def evaluatePostAggregateExprs(internalRow: InternalRow): InternalRow
  def evaluatePostFilter(row: InternalRow): Boolean

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

  private def className(dataType: DataType): String = {
    val tpe = dataType.classTag.toString()
    val lastDot = tpe.lastIndexOf(".")
    tpe.substring(lastDot + 1)
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
      case TypeKind.Array => tq"Seq[Expression[Any]]"
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

  private def mkGet(queryContext: QueryContext, row: TermName, e: Expression[_]): Tree = {
    val tpe = mkType(e)
    e match {
      case ConstantExpr(x) =>
        val lit = mapValue(e.dataType)(x)
        q"$lit.asInstanceOf[$tpe]"
      case x =>
        val idx = queryContext.exprsIndex(x)
        q"$row.get[$tpe]($idx)"
    }
  }

  private def mkSetUnary(
      queryContext: QueryContext,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      f: Tree => Tree,
      elseTree: Option[Tree] = None
  ): Tree = {
    val prepare = mkSet(queryContext, row, a).toSeq
    val getA = mkGet(queryContext, row, a)
    val idx = queryContext.exprsIndex(e)
    val tpe = mkType(e)
    val v = f(getA)
    elseTree match {
      case Some(e) =>
        q"""
          ..$prepare
          $row.set($idx, if ($getA != null) $v.asInstanceOf[$tpe] else $e.asInstanceOf[$tpe])
        """

      case None =>
        q"""
          ..$prepare
          if ($getA != null) $row.set($idx, $v.asInstanceOf[$tpe])
        """
    }
  }

  def mkSetMathUnary(
      queryContext: QueryContext,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      fun: TermName
  ): Tree = {
    if (a.dataType.integral.nonEmpty) {
      mkSetUnary(queryContext, row, e, a, x => q"${integralValName(a.dataType)}.$fun($x)")
    } else {
      mkSetUnary(queryContext, row, e, a, x => q"${fractionalValName(a.dataType)}.$fun($x)")
    }
  }

  private def mkSetBinary(
      queryContext: QueryContext,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      b: Expression[_],
      f: (Tree, Tree) => Tree
  ): Tree = {
    val prepare = Seq(mkSet(queryContext, row, a), mkSet(queryContext, row, b)).flatten
    val getA = mkGet(queryContext, row, a)
    val getB = mkGet(queryContext, row, b)
    val idx = queryContext.exprsIndex(e)
    val tpe = mkType(e)
    val v = f(getA, getB)
    q"""
       ..$prepare
       if ($getA != null && $getB != null) $row.set($idx, $v.asInstanceOf[$tpe])
       """
  }

  private def mkSetTypeConvertExpr(
      qc: QueryContext,
      row: TermName,
      e: Expression[_],
      a: Expression[_]
  ): Tree = {
    val mapper = typeConverters.getOrElse(
      (a.dataType.meta.sqlTypeName, e.dataType.meta.sqlTypeName),
      throw new IllegalArgumentException(s"Unsupported type conversion ${a.dataType} to ${e.dataType}")
    )
    val prepare = mkSet(qc, row, a).toSeq
    val getA = mkGet(qc, row, a)
    val idx = qc.exprsIndex(e)
    val tpe = mkType(e)
    val v = mapper(getA)
    q"""
       ..$prepare
       if ($getA != null) $row.set($idx, $v.asInstanceOf[$tpe])
       """
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
      queryContext: QueryContext,
      row: TermName,
      e: Expression[_],
      cs: Seq[Condition],
      reducer: Tree
  ): Tree = {
    val idx = queryContext.exprsIndex(e)
    val sets = cs.flatMap(c => mkSet(queryContext, row, c))
    val gets = cs.map(c => mkGet(queryContext, row, c))
    q"""..$sets
              val vs = List(..$gets)
              val res = vs.reduce($reducer)
              $row.set($idx, res)
            """
  }

  private def mkSet(qc: QueryContext, row: TermName, e: Expression[_]): Option[Tree] = {

    val t = e match {
      case ConstantExpr(c) =>
        val v = mapValue(e.dataType)(c)
        qc.exprsIndex.get(e).map(idx => q"$row.set($idx, $v)")
      case TimeExpr             => None
      case DimensionExpr(_)     => None
      case DimensionIdExpr(_)   => None
      case MetricExpr(_)        => None
      case LinkExpr(_, _)       => None
      case DimIdInExpr(_, _)    => None
      case DimIdNotInExpr(_, _) => None

      case TupleExpr(a, b) => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"($x, $y)"))

      case ae: AggregateExpr[_, _, _]   => mkSet(qc, row, ae.expr)
      case we: WindowFunctionExpr[_, _] => mkSet(qc, row, we.expr)

      case GtExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x > $y"""))
      case LtExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x < $y"""))
      case GeExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x >= $y"""))
      case LeExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x <= $y"""))
      case EqExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x == $y"""))
      case NeqExpr(a, b) => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x != $y"""))

      case InExpr(v, _)    => Some(mkSetUnary(qc, row, e, v, x => q"""${exprValName(e)}.contains($x)"""))
      case NotInExpr(v, _) => Some(mkSetUnary(qc, row, e, v, x => q"""!${exprValName(e)}.contains($x)"""))

      case PlusExpr(a, b)    => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x + $y"""))
      case MinusExpr(a, b)   => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x - $y"""))
      case TimesExpr(a, b)   => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x * $y"""))
      case DivIntExpr(a, b)  => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x / $y"""))
      case DivFracExpr(a, b) => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"""$x / $y"""))

      case TypeConvertExpr(_, a) => Some(mkSetTypeConvertExpr(qc, row, e, a))

      case TruncYearExpr(a)     => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.year())($x)"""))
      case TruncMonthExpr(a)    => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.monthOfYear())($x)"""))
      case TruncWeekExpr(a)     => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.weekOfWeekyear())($x)"""))
      case TruncDayExpr(a)      => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.dayOfMonth())($x)"""))
      case TruncHourExpr(a)     => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.hourOfDay())($x)"""))
      case TruncMinuteExpr(a)   => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.minuteOfHour())($x)"""))
      case TruncSecondExpr(a)   => Some(mkSetUnary(qc, row, e, a, x => q"""$truncTime($dtft.secondOfMinute())($x)"""))
      case ExtractYearExpr(a)   => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getYear"))
      case ExtractMonthExpr(a)  => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getMonthOfYear"))
      case ExtractDayExpr(a)    => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getDayOfMonth"))
      case ExtractHourExpr(a)   => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getHourOfDay"))
      case ExtractMinuteExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getMinuteOfHour"))
      case ExtractSecondExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLocalDateTime.getSecondOfMinute"))

      case TimeMinusExpr(a, b) =>
        Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)"))
      case TimeMinusPeriodExpr(a, b) =>
        Some(mkSetBinary(qc, row, e, a, b, (t, p) => q"Time($t.toDateTime.minus($p).getMillis)"))
      case TimePlusPeriodExpr(a, b) =>
        Some(mkSetBinary(qc, row, e, a, b, (t, p) => q"Time($t.toDateTime.plus($p).getMillis)"))
      case PeriodPlusPeriodExpr(a, b) => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"$x plus $y"))

      case IsNullExpr(a)    => Some(mkSetUnary(qc, row, e, a, _ => q"false", Some(q"true")))
      case IsNotNullExpr(a) => Some(mkSetUnary(qc, row, e, a, _ => q"true", Some(q"false")))

      case LowerExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"$x.toLowerCase"))
      case UpperExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"$x.toUpperCase"))

      case ConditionExpr(c, p, n) =>
        val prepare = Seq(mkSet(qc, row, c), mkSet(qc, row, p), mkSet(qc, row, n)).flatten
        val getC = mkGet(qc, row, c)
        val getP = mkGet(qc, row, p)
        val getN = mkGet(qc, row, n)
        val idx = qc.exprsIndex(e)
        Some(q"""..$prepare
             $row.set($idx, if ($getC) $getP else $getN)
             """)

      case AbsExpr(a)        => Some(mkSetMathUnary(qc, row, e, a, TermName("abs")))
      case UnaryMinusExpr(a) => Some(mkSetMathUnary(qc, row, e, a, TermName("negate")))

      case NotExpr(a)  => Some(mkSetUnary(qc, row, e, a, x => q"!$x"))
      case AndExpr(cs) => Some(mkLogical(qc, row, e, cs, q"_ && _"))
      case OrExpr(cs)  => Some(mkLogical(qc, row, e, cs, q"_ || _"))

      case SplitExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"tokenizer.transliteratedTokens($x)"))
      case TokensExpr(a) =>
        Some(mkSetUnary(qc, row, e, a, x => q"ExpressionCalculator.splitBy($x, !_.isLetterOrDigit).toSeq"))
      case LengthExpr(a)    => Some(mkSetUnary(qc, row, e, a, x => q"$x.length"))
      case ConcatExpr(a, b) => Some(mkSetBinary(qc, row, e, a, b, (x, y) => q"$x + $y"))

      case ArrayExpr(exprs) =>
        val sets = exprs.map(a => mkSet(qc, row, a))
        val gets = exprs.map(a => mkGet(qc, row, a))
        val idx = qc.exprsIndex(e)
        Some(q"""
            ..$sets
            $row.set($idx, Seq(..$gets))
           """)

      case ArrayLengthExpr(a)   => Some(mkSetUnary(qc, row, e, a, x => q"$x.size"))
      case ArrayToStringExpr(a) => Some(mkSetUnary(qc, row, e, a, x => q"""$x.mkString(", ")"""))
      case ArrayTokensExpr(a) =>
        Some(mkSetUnary(qc, row, e, a, x => q"""$x.flatMap(s => tokenizer.transliteratedTokens(s))"""))

      case ContainsExpr(as, b)     => Some(mkSetBinary(qc, row, e, as, b, (x, y) => q"$x.contains($y)"))
      case ContainsAnyExpr(as, bs) => Some(mkSetBinary(qc, row, e, as, bs, (x, y) => q"$y.exists($x.contains)"))
      case ContainsAllExpr(as, bs) => Some(mkSetBinary(qc, row, e, as, bs, (x, y) => q"$y.forall($x.contains)"))
      case ContainsSameExpr(as, bs) =>
        Some(mkSetBinary(qc, row, e, as, bs, (x, y) => q"$x.size == $y.size && $x.toSet == $y.toSet"))
    }

    t
  }

  private def mkFilter(queryContext: QueryContext, row: TermName, condition: Option[Expression.Condition]): Tree = {
    condition match {
      case Some(ConstantExpr(v)) => q"$v"

      case Some(cond) =>
        val get = mkGet(queryContext, row, cond)
        mkSet(queryContext, row, cond) match {
          case Some(eval) =>
            q"""
              $eval
              $get
             """
          case None => get
        }

      case None => q"true"
    }
  }

  private def mkEvaluate(qc: QueryContext, row: TermName): Tree = {
    val trees = qc.query.fields.map(_.expr).toList.flatMap(e => mkSet(qc, row, e))
    q"..$trees"
  }

  private def mkMap(qc: QueryContext, row: TermName): Tree = {
    val trees = qc.aggregateExprs.toSeq.map { ae =>
      val idx = qc.exprsIndex(ae)

      val exprValue = mkGet(qc, row, ae.expr)

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

    q"..$trees"
  }

  private def mkReduce(qc: QueryContext, rowA: TermName, rowB: TermName, outRow: TermName): Tree = {
    val trees = qc.aggregateExprs.toSeq.map { ae =>
      val idx = qc.exprsIndex(ae)
      val valueTpe = mkType(ae.expr)

      val aValue = mkGet(qc, rowA, ae)
      val bValue = mkGet(qc, rowB, ae)

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

  private def mkPostMap(qc: QueryContext, row: TermName): Tree = {
    val trees = qc.aggregateExprs.toSeq.flatMap { ae =>
      val idx = qc.exprsIndex(ae)
      val valueTpe = mkType(ae.expr)

      val oldValue = mkGet(qc, row, ae)

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
              val n = util.Random.nextInt(s.size)
              s.iterator.drop(n).next
            """
          )
      }

      value.map(v => q"$row.set($idx, $v)")
    }

    q"..$trees"
  }

  private def mkPostAggregate(queryContext: QueryContext, row: TermName): Tree = {
    val trees = queryContext.exprsOnAggregatesAndWindows.toSeq.flatMap(e => mkSet(queryContext, row, e))

    q"..$trees"
  }

  private def mkVars(queryContext: QueryContext): Seq[Tree] = {
    def setVariable[T: TypeTag](e: Expression[_], inner: Expression[_], values: Set[T]): Tree = {

      val literals = values.toList.map(mapValue(inner.dataType))
      val v = Apply(Ident(TermName("Set")), literals)
      val tpe = mkType(inner)

      q"private val ${exprValName(e)}: Set[$tpe] = $v"
    }

    val inVars = queryContext.exprsIndex.keys.collect {
      case e @ InExpr(i, values)    => setVariable(e, i, values)
      case e @ NotInExpr(i, values) => setVariable(e, i, values)
    }.toSeq

    val dtVars = queryContext.exprsIndex.keySet.map(_.dataType).toList.flatMap { dt =>
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

  def generateCalculator(queryContext: QueryContext, condition: Option[Condition]): Tree = {

    val defs = mkVars(queryContext)

    val internalRow = TermName("internalRow")
    val filter = mkFilter(queryContext, internalRow, condition)

    val evaluate = mkEvaluate(queryContext, internalRow)
    val map = mkMap(queryContext, internalRow)

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val outRow = TermName("reduced")
    val reduce = mkReduce(queryContext, rowA, rowB, outRow)

    val postMap = mkPostMap(queryContext, internalRow)
    val postAggregate = mkPostAggregate(queryContext, internalRow)

    val postFilter = mkFilter(queryContext, internalRow, queryContext.query.postFilter)

    q"""
        import _root_.org.yupana.api.Time
        import _root_.org.yupana.api.types.DataType
        
        new _root_.org.yupana.core.ExpressionCalculator {
          ..$defs
        
          override def evaluateFilter($internalRow: _root_.org.yupana.core.model.InternalRow): Boolean = $filter
          
          override def evaluateExpressions($internalRow: _root_.org.yupana.core.model.InternalRow): _root_.org.yupana.core.model.InternalRow = {
            $evaluate
            $internalRow
          }
          
          override def evaluateMap($internalRow: _root_.org.yupana.core.model.InternalRow): _root_.org.yupana.core.model.InternalRow = {
            $map
            $internalRow
          }
          
          override def evaluateReduce(
            $rowA: _root_.org.yupana.core.model.InternalRow,
            $rowB: _root_.org.yupana.core.model.InternalRow
          ): _root_.org.yupana.core.model.InternalRow = {
            val $outRow = $rowA.copy
            $reduce
            $outRow
          }
    
          override def evaluatePostMap($internalRow: _root_.org.yupana.core.model.InternalRow): _root_.org.yupana.core.model.InternalRow = {
            $postMap
            $internalRow
          }
    
          override def evaluatePostAggregateExprs($internalRow: _root_.org.yupana.core.model.InternalRow): _root_.org.yupana.core.model.InternalRow = {
            $postAggregate
            $internalRow
          }

          override def evaluatePostFilter($internalRow: _root_.org.yupana.core.model.InternalRow): Boolean = $postFilter
        }
    """
  }

  def makeCalculator(queryContext: QueryContext, condition: Option[Condition]): ExpressionCalculator = {
    val tb = currentMirror.mkToolBox()

    val tree = generateCalculator(queryContext, condition)

    logger.whenTraceEnabled {
      val index = queryContext.exprsIndex.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      index.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    tb.compile(tree)().asInstanceOf[ExpressionCalculator]
  }

  private def prettyTree(tree: Tree): String = {
    show(tree)
      .replaceAll("_root_\\.([^. ]+\\.)+", "")
      .replaceAll("\\.\\$bang\\$eq", " != ")
      .replaceAll("\\.\\$eq\\$eq", " == ")
      .replaceAll("\\.\\$amp\\$amp", " && ")
      .replaceAll("\\.\\$plus\\$plus", " ++ ")
      .replaceAll("\\.\\$plus", " + ")
      .replaceAll("\\.\\$minus", " - ")
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
