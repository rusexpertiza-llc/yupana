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

trait ExpressionCalculator {
  def preEvaluated[T](expr: Expression[T], queryContext: QueryContext, internalRow: InternalRow): T = {
    expr match {
      case ConstantExpr(v) => v
      case _               => internalRow.get[T](queryContext, expr)
    }
  }

  def evaluateFilter(row: InternalRow): Boolean
  def evaluateExpressions(internalRow: InternalRow): InternalRow
  def evaluateMap(internalRow: InternalRow): InternalRow
  def evaluateReduce(a: InternalRow, b: InternalRow): InternalRow
  def evaluatePostMap(internalRow: InternalRow): InternalRow
  def evaluatePostAggregateExprs(internalRow: InternalRow): InternalRow

//  def evaluateExpression[T](expr: Expression[T], queryContext: QueryContext, internalRow: InternalRow): T

//  def evaluateMap[I, M](expr: AggregateExpr[I, M, _], queryContext: QueryContext, row: InternalRow): M
//  def evaluateReduce[M](expr: AggregateExpr[_, M, _], queryContext: QueryContext, a: InternalRow, b: InternalRow): M
//  def evaluatePostMap[M, O](expr: AggregateExpr[_, M, O], queryContext: QueryContext, row: InternalRow): O

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O
}

object ExpressionCalculator extends StrictLogging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox

  implicit private val timeLiftable: Liftable[Time] = Liftable[Time] { t => q"_root_.org.yupana.api.Time(${t.millis})" }

  private def className(dataType: DataType): String = {
    val tpe = dataType.classTag.toString()
    val lastDot = tpe.lastIndexOf(".")
    tpe.substring(lastDot + 1)
  }

  private def mkType(e: Expression[_]): Tree = {
    e.dataType.kind match {
      case TypeKind.Regular => Ident(TypeName(className(e.dataType)))
      case TypeKind.Tuple =>
        val tt = e.dataType.asInstanceOf[TupleDataType[_, _]]
        AppliedTypeTree(
          Ident(TypeName("Tuple2")),
          List(Ident(TypeName(className(tt.aType))), Ident(TypeName(className(tt.bType))))
        )
      case TypeKind.Array => tq"Seq[Expression[Any]]"
    }
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

  private val truncTime = q"_root_.org.yupana.core.ExpressionCalculator.truncateTime"
  private val dtft = q"_root_.org.joda.time.DateTimeFieldType"

  private def exprValName(e: Expression[_]): TermName = {
    TermName(s"e_${e.hashCode()}")
  }

  private def mkSet(queryContext: QueryContext, row: TermName, e: Expression[_]): Option[Tree] = {

    val t = e match {
      case ConstantExpr(_)      => None
      case TimeExpr             => None
      case DimensionExpr(_)     => None
      case DimensionIdExpr(_)   => None
      case MetricExpr(_)        => None
      case LinkExpr(_, _)       => None
      case DimIdInExpr(_, _)    => None
      case DimIdNotInExpr(_, _) => None

      case TupleExpr(a, b) => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"($x, $y)"))

      case ae: AggregateExpr[_, _, _]   => mkSet(queryContext, row, ae.expr)
      case we: WindowFunctionExpr[_, _] => mkSet(queryContext, row, we.expr)

      case GtExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x > $y"""))
      case LtExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x < $y"""))
      case GeExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x >= $y"""))
      case LeExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x <= $y"""))
      case EqExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x == $y"""))
      case NeqExpr(a, b) => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x != $y"""))

      case InExpr(v, _)    => Some(mkSetUnary(queryContext, row, e, v, x => q"""${exprValName(e)}.contains($x)"""))
      case NotInExpr(v, _) => Some(mkSetUnary(queryContext, row, e, v, x => q"""!${exprValName(e)}.contains($x)"""))

      case PlusExpr(a, b)    => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x + $y"""))
      case MinusExpr(a, b)   => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x - $y"""))
      case TimesExpr(a, b)   => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x * $y"""))
      case DivIntExpr(a, b)  => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x / $y"""))
      case DivFracExpr(a, b) => Some(mkSetBinary(queryContext, row, e, a, b, (x, y) => q"""$x / $y"""))

      case TruncYearExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.year())($x)"""))
      case TruncMonthExpr(a) =>
        Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.monthOfYear())($x)"""))
      case TruncWeekExpr(a) =>
        Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.weekOfWeekyear())($x)"""))
      case TruncDayExpr(a)  => Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.dayOfMonth())($x)"""))
      case TruncHourExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.hourOfDay())($x)"""))
      case TruncMinuteExpr(a) =>
        Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.minuteOfHour())($x)"""))
      case TruncSecondExpr(a) =>
        Some(mkSetUnary(queryContext, row, e, a, x => q"""$truncTime($dtft.secondOfMinute())($x)"""))

      case IsNullExpr(a)    => Some(mkSetUnary(queryContext, row, e, a, _ => q"false", Some(q"true")))
      case IsNotNullExpr(a) => Some(mkSetUnary(queryContext, row, e, a, _ => q"true", Some(q"false")))

      case LowerExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"$x.toLowerCase"))
      case UpperExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"$x.toUpperCase"))

      case ConditionExpr(c, p, n) =>
        val prepare = Seq(mkSet(queryContext, row, c), mkSet(queryContext, row, p), mkSet(queryContext, row, n)).flatten
        val getC = mkGet(queryContext, row, c)
        val getP = mkGet(queryContext, row, p)
        val getN = mkGet(queryContext, row, n)
        val idx = queryContext.exprsIndex(e)
        Some(q"""..$prepare
             $row.set($idx, if ($getC) $getP else $getN)
             """)

      case AbsExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"_root_.scala.math.abs($x)"))

      case NotExpr(a) => Some(mkSetUnary(queryContext, row, e, a, x => q"!$x"))
      case AndExpr(cs) =>
        val idx = queryContext.exprsIndex(e)
        val sets = cs.flatMap(c => mkSet(queryContext, row, c))
        val gets = cs.map(c => mkGet(queryContext, row, c))
        Some(q"""..$sets
              val vs = List(..$gets)
              val res = vs.reduce(_ && _)
              $row.set($idx, res)
            """)

    }

    t
  }

  def mkFilter(queryContext: QueryContext, row: TermName, condition: Option[Expression.Condition]): Tree = {
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

  def mkEvaluate(qc: QueryContext, row: TermName): Tree = {
    val trees = qc.query.fields.map(_.expr).toList.flatMap(e => mkSet(qc, row, e))
    q"..$trees"
  }

  def mkMap(qc: QueryContext, row: TermName): Tree = {
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

  def mkReduce(qc: QueryContext, rowA: TermName, rowB: TermName, outRow: TermName): Tree = {
    val trees = qc.aggregateExprs.toSeq.map { ae =>
      val idx = qc.exprsIndex(ae)
      val valueTpe = mkType(ae.expr)

      val aValue = mkGet(qc, rowA, ae)
      val bValue = mkGet(qc, rowB, ae)

      val value = ae match {
        case SumExpr(_)            => q"$aValue + $bValue"
        case MinExpr(_)            => q"_root_.scala.math.min($aValue, $bValue)"
        case MaxExpr(_)            => q"_root_.scala.math.max($aValue, $bValue)"
        case CountExpr(_)          => q"$rowA.get[Long]($idx) + $rowB.get[Long]($idx)"
        case DistinctCountExpr(_)  => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
        case DistinctRandomExpr(_) => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
      }
      q"$outRow.set($idx, $value)"
    }

    q"..$trees"
  }

  def mkPostMap(qc: QueryContext, row: TermName): Tree = {
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

  def mkPostAggregate(queryContext: QueryContext, row: TermName): Tree = {
    val trees = queryContext.exprsOnAggregatesAndWindows.toSeq.flatMap { e =>
      val winExprsIndices = e.flatten.collect { case w: WindowFunctionExpr[_, _] => queryContext.exprsIndex(w) }
      mkSet(queryContext, row, e).map { set =>
        q"""if ($winExprsIndices.forall((x: Int) => $row.isEmpty(x))) $set"""
      }
    }

    q"..$trees"
  }

  def mkVars(queryContext: QueryContext): Seq[Tree] = {
    def setVariable[T: TypeTag](e: Expression[_], inner: Expression[_], values: Set[T]): Tree = {

      val literals = values.toList.map(mapValue(inner.dataType))
      val v = Apply(Ident(TermName("Set")), literals)
      val tpe = mkType(inner)

      q"private val ${exprValName(e)}: Set[$tpe] = $v"
    }

    queryContext.exprsIndex.keys.collect {
      case e @ InExpr(i, values)    => setVariable(e, i, values)
      case e @ NotInExpr(i, values) => setVariable(e, i, values)
    }.toSeq
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

    q"""
        import _root_.org.yupana.api.Time
        
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

          def evaluateWindow[I, O](winFuncExpr: _root_.org.yupana.api.query.WindowFunctionExpr[I, O], values: Array[I], index: Int): O = ???
        }
    """
  }

  def makeCalculator(queryContext: QueryContext, condition: Option[Condition]): ExpressionCalculator = {
    val tb = currentMirror.mkToolBox()

    val tree = generateCalculator(queryContext, condition)

    logger.whenTraceEnabled {
      val index = queryContext.exprsIndex.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      index.foreach(s => logger.trace(s"  ${s}"))
      logger.trace(s"Tree: ${show(tree)}")
    }

    tb.compile(tree)().asInstanceOf[ExpressionCalculator]
  }

  def truncateTime(fieldType: DateTimeFieldType)(time: Time): Time = {
    Time(time.toDateTime.property(fieldType).roundFloorCopy().getMillis)
  }

}
