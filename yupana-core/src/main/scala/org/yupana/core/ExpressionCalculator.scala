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

import java.sql.Types
import scala.collection.AbstractIterator

trait ExpressionCalculator {
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
  private val params = TermName("params")
  private val calculator = q"_root_.org.yupana.core.ExpressionCalculator"

  case class Decl(name: TermName, tpe: Tree, value: Tree)
  case class LocalDecl(e: Expression[_], name: TermName, value: Tree, defTree: Option[Tree]) {
    val defName: TermName = TermName(name.toString + "_def")

    val isDefined: Option[Tree] = {
      if (defTree.isDefined) Some(q"$defName") else None
    }
  }

  case class State(
      index: Map[Expression[_], Int],
      required: Set[Expression[_]],
      unfinished: Set[Expression[_]],
      refs: Seq[(AnyRef, Decl)],
      globalDecls: Seq[(Any, Decl)],
      localDecls: Seq[LocalDecl],
      trees: Seq[Tree],
      exprId: Int
  ) {
    def withRequired(e: Expression[_]): State = copy(required = required + e).withExpr(e)

    def withExpr(e: Expression[_]): State = {
      if (index.contains(e)) this else this.copy(index = this.index + (e -> index.size))
    }

    def withDefine(row: TermName, e: Expression[_], v: Tree): State = {
      if (required.contains(e)) {
        val newState = withExpr(e)
        val idx = newState.index(e)
        newState.copy(trees = q"$row.set($idx, $v)" +: trees)
      } else if (!localDecls.exists(_.e == e)) {
        val decl = LocalDecl(e, nextLocalName, v, None)
        copy(localDecls = decl +: localDecls)
      } else {
        this
      }
    }

    def withUnfinished(e: Expression[_]): State = copy(unfinished = unfinished + e)

    def withDefineIf(row: TermName, e: Expression[_], cond: Tree, v: Tree): State = {
      if (required.contains(e)) {
        val newState = withExpr(e)
        val idx = newState.index(e)
        val tree = q"if ($cond) $row.set($idx, $v)"
        newState.withExpr(e).copy(trees = tree +: trees)
      } else if (!localDecls.exists(_.e == e)) {
        val decl = LocalDecl(e, nextLocalName, v, Some(cond))
        copy(localDecls = decl +: localDecls)
      } else {
        this
      }
    }

    def withNamedGlobal(name: TermName, tpe: Tree, tree: Tree): State = {
      if (!globalDecls.exists(_._1 == name))
        copy(globalDecls = globalDecls :+ (name -> Decl(name, tpe, tree)))
      else this
    }

    def withRef(ref: AnyRef, tpe: Tree): (TermName, State) = {
      refs.find(_._1 == ref) match {
        case Some((_, Decl(name, _, _))) => name -> this
        case None =>
          val name = TermName(s"e_$exprId")
          val refId = refs.size
          val tree = q"$params($refId)"
          val ns = copy(refs = refs :+ (ref -> Decl(name, tpe, tree)), exprId = exprId + 1)
          name -> ns
      }
    }

    def withGlobal(key: Any, tpe: Tree, tree: Tree): (TermName, State) = {
      globalDecls.find(_._1 == key) match {
        case Some((_, Decl(name, _, _))) => name -> this
        case None =>
          val name = TermName(s"e_$exprId")
          val ns =
            copy(globalDecls = globalDecls :+ (key -> Decl(name, tpe, tree)), exprId = exprId + 1)
          name -> ns
      }
    }

    def appendLocal(ts: Tree*): State = copy(trees = ts.reverse ++ trees)

    def fresh: (Tree, State) = {
      val locals = localDecls.reverse.flatMap {
        case decl @ LocalDecl(e, n, v, d) =>
          val tpe = mkType(e)
          val defTree = d.map(t => q"val ${decl.defName}: Boolean = $t")
          val default = mkDefault(e.dataType)
          val tree = if (defTree.isDefined) q"if (${decl.defName}) $v else $default.asInstanceOf[$tpe]" else v

          Seq(defTree, Some(q"val $n: $tpe = $tree")).flatten
      }

      val res =
        q"""
            ..$locals
            ..${trees.reverse}
          """

      val newState = State(index, required, unfinished, refs, globalDecls, Seq.empty, Seq.empty, exprId)

      res -> newState
    }

    private def nextLocalName: TermName = TermName(s"l_${localDecls.size}")
  }

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

  private def mkZero(state: State, dataType: DataType): (Tree, State) = {
    dataType.numeric match {
      case Some(n) => mapValue(state, dataType)(n.zero)
      case None    => throw new IllegalArgumentException(s"$dataType is not numeric")
    }
  }

  private def mkDefault(dataType: DataType): Tree = {
    if (dataType.classTag.runtimeClass.isPrimitive) {
      if (dataType == DataType[Boolean]) q"false"
      else
        dataType.numeric
          .map(_ => q"0")
          .getOrElse(throw new IllegalArgumentException(s"Unexpected primitive type $dataType"))
    } else q"null"
  }

  private def mapValue(state: State, tpe: DataType)(v: Any): (Tree, State) = {
    import scala.reflect.classTag

    tpe.kind match {
      case TypeKind.Regular if tpe.classTag == classTag[Time] || tpe.classTag == classTag[BigDecimal] =>
        val (name, ns) = state.withRef(v.asInstanceOf[AnyRef], mkType(tpe))
        q"$name" -> ns

      case TypeKind.Regular => Literal(Constant(v)) -> state

      case TypeKind.Tuple =>
        val tt = tpe.asInstanceOf[TupleDataType[_, _]]
        val (a, b) = v.asInstanceOf[(_, _)]
        val (aTree, s1) = mapValue(state, tt.aType)(a)
        val (bTree, s2) = mapValue(s1, tt.bType)(b)
        (Apply(Ident(TermName("Tuple2")), List(aTree, bTree)), s2)

      case TypeKind.Array =>
        val (c, s) =
          mkSeqValue(state, tpe.asInstanceOf[ArrayDataType[_]].valueType, v.asInstanceOf[Traversable[_]])
        val (name, ns) = s.withGlobal(v, mkType(tpe), c)
        q"$name" -> ns
    }
  }

  private def mkGet(state: State, row: TermName, e: Expression[_]): Option[(Tree, State)] = {
    val tpe = mkType(e)
    e match {
      case x if state.unfinished.contains(x) => None

      case ConstantExpr(x) =>
        val (v, ns) = mapValue(state, e.dataType)(x)
        Some(q"$v.asInstanceOf[$tpe]" -> ns)
      case ae @ ArrayExpr(exprs) if e.kind == Const =>
        val (lits, newState) = exprs.foldLeft((Seq.empty[Tree], state)) {
          case ((ts, s), ConstantExpr(v)) =>
            val (t, ns) = mapValue(s, ae.elementDataType)(v)
            (ts :+ t, ns)
          case (_, x) => throw new IllegalArgumentException(s"Unexpected constant expression $x")
        }
        val innerTpe = mkType(ae.elementDataType)
        Some(q"Seq[$innerTpe](..$lits)" -> newState)

      case x if state.index.contains(x) =>
        val idx = state.index(x)
        Some(q"$row.get[$tpe]($idx)" -> state)

      case x =>
        state.localDecls
          .find(_.e == x)
          .map(x => q"${x.name}" -> state)
    }
  }

  private def mkGetNow(state: State, row: TermName, e: Expression[_]): (Tree, State) = {
    mkGet(state, row, e) getOrElse (throw new IllegalStateException(s"Unknown expression $e"))
  }

  private def mkIsDefined(state: State, row: TermName, e: Expression[_]): Option[Tree] = {
    e match {
      case ConstantExpr(_)  => None
      case TimeExpr         => None
      case DimensionExpr(_) => None
      case TupleExpr(e1, e2) =>
        val d1 = mkIsDefined(state, row, e1)
        val d2 = mkIsDefined(state, row, e2)

        (d1, d2) match {
          case (Some(t1), Some(t2)) => Some(q"$t1 && $t2")
          case _                    => d1 orElse d2
        }

      case _ =>
        state.index
          .get(e)
          .map(idx => q"$row.isDefined($idx)")
          .orElse(state.localDecls.find(_.e == e).flatMap(_.isDefined))
    }
  }

  private def mkSetUnary(
      state: State,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      f: Tree => Tree,
      elseTree: Option[Tree] = None
  ): State = {
    val preparedState = mkSet(state, row, a)
    mkGet(preparedState, row, a).map {
      case (getA, newState) =>
        val tpe = mkType(e)
        val v = f(getA)
        mkIsDefined(newState, row, a) match {
          case Some(aIsDefined) =>
            elseTree match {
              case Some(d) =>
                newState.withDefine(row, e, q"if ($aIsDefined) $v.asInstanceOf[$tpe] else $d.asInstanceOf[$tpe]")

              case None =>
                newState.withDefineIf(row, e, aIsDefined, q"$v.asInstanceOf[$tpe]")
            }
          case None =>
            newState.withDefine(row, e, q"$v.asInstanceOf[$tpe]")
        }
    } getOrElse preparedState
  }

  private def mkSetMathUnary(state: State, row: TermName, e: Expression[_], a: Expression[_], fun: TermName): State = {
    val aType = mkType(a)
    if (a.dataType.integral.nonEmpty) {
      mkSetUnary(state, row, e, a, x => q"${integralValName(a.dataType)}.$fun($x)")
        .withNamedGlobal(
          integralValName(a.dataType),
          tq"Integral[$aType]",
          q"DataType.bySqlName(${e.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].integral.get"
        )
    } else {
      mkSetUnary(state, row, e, a, x => q"${fractionalValName(a.dataType)}.$fun($x)")
        .withNamedGlobal(
          fractionalValName(a.dataType),
          tq"Fractional[$aType]",
          q"DataType.bySqlName(${e.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].fractional.get"
        )
    }
  }

  private def mkSetTypeConvertExpr(state: State, row: TermName, e: Expression[_], a: Expression[_]): State = {
    val mapper = typeConverters.getOrElse(
      (a.dataType.meta.sqlTypeName, e.dataType.meta.sqlTypeName),
      throw new IllegalArgumentException(s"Unsupported type conversion ${a.dataType} to ${e.dataType}")
    )
    mkSetUnary(state, row, e, a, mapper)
  }

  private def mkSetBinary(
      state: State,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      b: Expression[_],
      f: (Tree, Tree) => Tree
  ): State = {
    val preparedState = mkSetExprs(state, row, Seq(a, b))

    val res = for {
      getA <- mkGet(preparedState, row, a)
      getB <- mkGet(getA._2, row, b)
    } yield {
      val tpe = mkType(e)
      val v = q"${f(getA._1, getB._1)}.asInstanceOf[$tpe]"
      val newState = getB._2

      (mkIsDefined(newState, row, a), mkIsDefined(newState, row, b)) match {
        case (Some(aDefined), Some(bDefined)) =>
          newState.withDefineIf(row, e, q"$aDefined && $bDefined", v)
        case (Some(aDefined), None) =>
          newState.withDefineIf(row, e, q"$aDefined", v)
        case (None, Some(bDefined)) =>
          newState.withDefineIf(row, e, q"$bDefined", v)
        case (None, None) =>
          newState.withDefine(row, e, v)
      }
    }

    res getOrElse preparedState
  }

  private def mkDivFrac(state: State, row: TermName, e: Expression[_], a: Expression[_], b: Expression[_]): State = {
    if (a.dataType.meta.sqlType != Types.DECIMAL) mkSetBinary(state, row, e, a, b, (x, y) => q"$x / $y")
    else {
      val scale = a.dataType.meta.scale
      mkSetBinary(
        state,
        row,
        e,
        a,
        b,
        (x, y) =>
          q"new BigDecimal($x.bigDecimal.divide($y.bigDecimal, $scale, _root_.java.math.RoundingMode.HALF_EVEN))"
      )
    }
  }

  private def mkSetOrd(
      state: State,
      row: TermName,
      e: Expression[_],
      a: Expression[_],
      b: Expression[_],
      f: (Tree, Tree) => Tree,
      ordFunName: TermName
  ): State = {
    if (a.dataType.numeric.nonEmpty) {
      mkSetBinary(state, row, e, a, b, f)
    } else {
      val aType = mkType(a)
      val ord = ordValName(a.dataType)
      val newState = state.withNamedGlobal(
        ord,
        tq"Ordering[$aType]",
        q"DataType.bySqlName(${a.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].ordering.get"
      )

      mkSetBinary(newState, row, e, a, b, (x, y) => q"$ord.$ordFunName($x, $y)")
    }
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
      state: State,
      row: TermName,
      e: Expression[_],
      cs: Seq[Condition],
      reducer: (Tree, Tree) => Tree
  ): State = {
    val preparedState = mkSetExprs(state, row, cs)
    val (gets, newState) = mkGetExprs(preparedState, row, cs)
    if (gets.nonEmpty)
      newState.withDefine(row, e, gets.reduceLeft(reducer))
    else newState
  }

  private def mkSet(state: State, row: TermName, e: Expression[_]): State = {

    if (state.index.contains(e)) {
      state
    } else {

      e match {
        case ConstantExpr(c) =>
          val (v, s) = mapValue(state, e.dataType)(c)
          if (s.required.contains(e)) s.withDefine(row, e, v) else s

        case TimeExpr             => state.withExpr(e)
        case DimensionExpr(_)     => state.withExpr(e)
        case DimensionIdExpr(_)   => state.withExpr(e)
        case MetricExpr(_)        => state.withExpr(e)
        case DimIdInExpr(_, _)    => state.withExpr(e)
        case DimIdNotInExpr(_, _) => state.withExpr(e)
        case LinkExpr(link, _) =>
          val dimExpr = DimensionExpr(link.dimension)
          state.withExpr(dimExpr).withExpr(e)

        case _: AggregateExpr[_, _, _] => state.withRequired(e).withUnfinished(e)

        case we: WindowFunctionExpr[_, _] =>
          val s1 = state.withRequired(we).withRequired(we.expr).withRequired(TimeExpr).withUnfinished(we)
          val (t, s2) = mkGetNow(s1, row, we.expr)
          s2.withDefine(row, we, t)

        case TupleExpr(a, b) => mkSetBinary(state, row, e, a, b, (x, y) => q"($x, $y)")

        case GtExpr(a, b)  => mkSetOrd(state, row, e, a, b, (x, y) => q"""$x > $y""", TermName("gt"))
        case LtExpr(a, b)  => mkSetOrd(state, row, e, a, b, (x, y) => q"""$x < $y""", TermName("lt"))
        case GeExpr(a, b)  => mkSetOrd(state, row, e, a, b, (x, y) => q"""$x >= $y""", TermName("gteq"))
        case LeExpr(a, b)  => mkSetOrd(state, row, e, a, b, (x, y) => q"""$x <= $y""", TermName("lteq"))
        case EqExpr(a, b)  => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x == $y""")
        case NeqExpr(a, b) => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x != $y""")

        case InExpr(v, vs) =>
          val (n, ns) = state.withRef(vs, tq"Set[${mkType(v)}]")
          mkSetUnary(ns, row, e, v, x => q"""$n.contains($x)""")

        case NotInExpr(v, vs) =>
          val (n, ns) = state.withRef(vs, tq"Set[${mkType(v)}]")
          mkSetUnary(ns, row, e, v, x => q"""!$n.contains($x)""")

        case PlusExpr(a, b)    => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x + $y""")
        case MinusExpr(a, b)   => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x - $y""")
        case TimesExpr(a, b)   => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x * $y""")
        case DivIntExpr(a, b)  => mkSetBinary(state, row, e, a, b, (x, y) => q"""$x / $y""")
        case DivFracExpr(a, b) => mkDivFrac(state, row, e, a, b)

        case TypeConvertExpr(_, a) => mkSetTypeConvertExpr(state, row, e, a)

        case TruncYearExpr(a) => mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.year())($x)""")
        case TruncMonthExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.monthOfYear())($x)""")
        case TruncWeekExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.weekOfWeekyear())($x)""")
        case TruncDayExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.dayOfMonth())($x)""")
        case TruncHourExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.hourOfDay())($x)""")
        case TruncMinuteExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.minuteOfHour())($x)""")
        case TruncSecondExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$truncTime($dtft.secondOfMinute())($x)""")
        case ExtractYearExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getYear")
        case ExtractMonthExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getMonthOfYear")
        case ExtractDayExpr(a)  => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getDayOfMonth")
        case ExtractHourExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getHourOfDay")
        case ExtractMinuteExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getMinuteOfHour")
        case ExtractSecondExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getSecondOfMinute")

        case TimeMinusExpr(a, b) =>
          mkSetBinary(state, row, e, a, b, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)")
        case TimeMinusPeriodExpr(a, b) =>
          mkSetBinary(state, row, e, a, b, (t, p) => q"Time($t.toDateTime.minus($p).getMillis)")
        case TimePlusPeriodExpr(a, b) =>
          mkSetBinary(state, row, e, a, b, (t, p) => q"Time($t.toDateTime.plus($p).getMillis)")
        case PeriodPlusPeriodExpr(a, b) => mkSetBinary(state, row, e, a, b, (x, y) => q"$x plus $y")

        case IsNullExpr(a)    => mkSetUnary(state, row, e, a, _ => q"false", Some(q"true"))
        case IsNotNullExpr(a) => mkSetUnary(state, row, e, a, _ => q"true", Some(q"false"))

        case LowerExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLowerCase")
        case UpperExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toUpperCase")

        case ConditionExpr(c, p, n) =>
          val newState = mkSetExprs(state, row, Seq(c, p, n))
          val (getC, cState) = mkGetNow(newState, row, c)
          val (getP, pState) = mkGetNow(cState, row, p)
          val (getN, nState) = mkGetNow(pState, row, n)
          nState.withDefine(row, e, q"if ($getC) $getP else $getN")

        case AbsExpr(a)        => mkSetMathUnary(state, row, e, a, TermName("abs"))
        case UnaryMinusExpr(a) => mkSetMathUnary(state, row, e, a, TermName("negate"))

        case NotExpr(a)  => mkSetUnary(state, row, e, a, x => q"!$x")
        case AndExpr(cs) => mkLogical(state, row, e, cs, (a, b) => q"$a && $b")
        case OrExpr(cs)  => mkLogical(state, row, e, cs, (a, b) => q"$a || $b")

        case TokensExpr(a)    => mkSetUnary(state, row, e, a, x => q"$tokenizer.transliteratedTokens($x)")
        case SplitExpr(a)     => mkSetUnary(state, row, e, a, x => q"$calculator.splitBy($x, !_.isLetterOrDigit).toSeq")
        case LengthExpr(a)    => mkSetUnary(state, row, e, a, x => q"$x.length")
        case ConcatExpr(a, b) => mkSetBinary(state, row, e, a, b, (x, y) => q"$x + $y")

        case ArrayExpr(exprs) =>
          val newState = mkSetExprs(state, row, exprs)
          val (gets, newestState) = mkGetExprs(newState, row, exprs)
          newestState.withDefine(row, e, q"Seq(..$gets)")

        case ArrayLengthExpr(a)   => mkSetUnary(state, row, e, a, x => q"$x.size")
        case ArrayToStringExpr(a) => mkSetUnary(state, row, e, a, x => q"""$x.mkString(", ")""")
        case ArrayTokensExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"""$x.flatMap(s => $tokenizer.transliteratedTokens(s))""")

        case ContainsExpr(as, b) => mkSetBinary(state, row, e, as, b, (x, y) => q"$x.contains($y)")
        case ContainsAnyExpr(as, bs) =>
          mkSetBinary(state, row, e, as, bs, (x, y) => q"$y.exists($x.contains)")
        case ContainsAllExpr(as, bs) =>
          mkSetBinary(state, row, e, as, bs, (x, y) => q"$y.forall($x.contains)")
        case ContainsSameExpr(as, bs) =>
          mkSetBinary(state, row, e, as, bs, (x, y) => q"$x.size == $y.size && $x.toSet == $y.toSet")
      }
    }
  }

  private def mkFilter(state: State, row: TermName, condition: Option[Condition]): State = {
    condition match {
      case Some(ConstantExpr(v)) => state.appendLocal(q"$v")

      case Some(cond) =>
        val newState = mkSet(state, row, cond)
        mkGet(newState, row, cond) map {
          case (t, ns) =>
            ns.appendLocal(t)
        } getOrElse newState

      case None => state.appendLocal(q"true")
    }
  }

  private def mkSetExprs(state: State, row: TermName, exprs: Seq[Expression[_]]): State = {
    exprs.foldLeft(state) {
      case (s, c) => mkSet(s, row, c)
    }
  }

  private def mkGetExprs(state: State, row: TermName, exprs: Seq[Expression[_]]): (Seq[Tree], State) = {
    exprs.foldLeft((Seq.empty[Tree], state)) {
      case ((ts, s), e) =>
        val (t, ns) = mkGetNow(s, row, e)
        (ts :+ t, ns)
    }
  }

  private def mkEvaluate(
      query: Query,
      row: TermName,
      state: State
  ): State = {
    mkSetExprs(state, row, query.fields.map(_.expr).toList ++ query.groupBy)
  }

  private def mkMap(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): State = {
    val newState = mkSetExprs(state, row, aggregates.map(_.expr))

    aggregates.foldLeft(newState) { (s, ae) =>
      val (exprValue, s2) = mkGetNow(s, row, ae.expr)

      ae match {
        case SumExpr(_) => s2.withDefine(row, ae, exprValue)
        case MinExpr(_) | MaxExpr(_) =>
          val aType = mkType(ae.expr)
          s2.withDefine(row, ae, exprValue)
            .withNamedGlobal(
              ordValName(ae.expr.dataType),
              tq"Ordering[$aType]",
              q"DataType.bySqlName(${ae.expr.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].ordering.get"
            )

        case CountExpr(_) =>
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) => s.withDefine(row, ae, q"if ($d) 1L else 0L")
            case None    => s.withDefine(row, ae, q"1L")
          }

        case DistinctCountExpr(_) | DistinctRandomExpr(_) =>
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) => s.withDefine(row, ae, q"if ($d) Set($exprValue) else Set.empty")
            case None    => s.withDefine(row, ae, q"Set($exprValue)")
          }

      }
    }
  }

  private def mkSetReduce(
      state: State,
      rowA: TermName,
      rowB: TermName,
      ae: AggregateExpr[_, _, _],
      f: (Tree, Tree) => Tree
  ): State = {
    val (aValue, s1) = mkGetNow(state, rowA, ae)
    val (bValue, s2) = mkGetNow(s1, rowB, ae)

    (mkIsDefined(s2, rowA, ae), mkIsDefined(s2, rowB, ae)) match {
      case (Some(da), Some(db)) =>
        s2.withDefine(
          rowA,
          ae,
          q"""if ($da && $db) ${f(aValue, bValue)}
              else if ($da) $aValue
              else $bValue"""
        )
      case (Some(da), None) => s2.withDefine(rowA, ae, q"if ($da) ${f(aValue, bValue)} else $bValue")
      case (None, Some(db)) => s2.withDefine(rowA, ae, q"if ($db) ${f(aValue, bValue)} else $aValue")
      case (None, None)     => s2.withDefine(rowA, ae, f(aValue, bValue))
    }
  }

  private def mkReduce(state: State, aggregates: Seq[AggregateExpr[_, _, _]], rowA: TermName, rowB: TermName): State = {

    aggregates.foldLeft(state) { (s, ae) =>
      val idx = state.index(ae)
      val valueTpe = mkType(ae.expr)

      ae match {
        case SumExpr(_)   => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"$a + $b")
        case MinExpr(_)   => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"${ordValName(ae.expr.dataType)}.min($a, $b)")
        case MaxExpr(_)   => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"${ordValName(ae.expr.dataType)}.max($a, $b)")
        case CountExpr(_) => s.withDefine(rowA, ae, q"$rowA.get[Long]($idx) + $rowB.get[Long]($idx)")
        case DistinctCountExpr(_) =>
          s.withDefine(rowA, ae, q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)")
        case DistinctRandomExpr(_) =>
          s.withDefine(rowA, ae, q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)")
      }
    }
  }

  private def mkPostMap(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): State = {
    aggregates.foldLeft(state) { (s, ae) =>
      val idx = state.index(ae)
      val valueTpe = mkType(ae.expr)

      val (oldValue, _) = mkGetNow(state, row, ae)

      val valueAndState = ae match {
        case SumExpr(_) =>
          val (z, ns) = mkZero(s, ae.dataType)
          Some(mkIsDefined(state, row, ae).fold(oldValue)(d => q"if ($d) $oldValue else $z") -> ns)
        case MinExpr(_)           => None
        case MaxExpr(_)           => None
        case CountExpr(_)         => None
        case DistinctCountExpr(_) => Some(q"$row.get[Set[$valueTpe]]($idx).size" -> s)
        case DistinctRandomExpr(_) =>
          Some(
            q"""
              val s = $row.get[Set[$valueTpe]]($idx)
              val n = _root_.scala.util.Random.nextInt(s.size)
              s.iterator.drop(n).next
            """ -> s
          )
      }

      valueAndState map { case (v, ns) => ns.withDefine(row, ae, v) } getOrElse s
    }
  }

  private def mkPostAggregate(
      query: Query,
      row: TermName,
      state: State
  ): State = {
    mkSetExprs(state, row, query.fields.map(_.expr))
  }

  private def mkSeqValue[T: TypeTag](
      state: State,
      tpe: DataType,
      values: Traversable[T]
  ): (Tree, State) = {
    val (literals, newState) = values.toList.foldLeft((List.empty[Tree], state)) {
      case ((ts, s), v) =>
        val (t, ns) = mapValue(s, tpe)(v)
        (ts :+ t, ns)
    }
    Apply(Ident(TermName("Seq")), literals) -> newState
  }

  def generateCalculator(query: Query, condition: Option[Condition]): (Tree, Map[Expression[_], Int], Array[Any]) = {
    val internalRow = TermName("internalRow")
    val initialState =
      State(
        Map.empty,
        query.fields.map(_.expr).toSet ++ query.groupBy ++ query.postFilter + TimeExpr,
        Set.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        0
      )

    val (filter, filteredState) = mkFilter(initialState, internalRow, condition).fresh

    val (evaluate, evaluatedState) = mkEvaluate(query, internalRow, filteredState).fresh

    val knownAggregates = (evaluatedState.index.keySet ++ evaluatedState.required).collect {
      case ae: AggregateExpr[_, _, _] => ae
    }.toSeq

    val beforeAggregationState = evaluatedState.copy(unfinished = Set.empty)

    val (map, mappedState) = mkMap(beforeAggregationState, knownAggregates, internalRow).fresh

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val (reduce, reducedState) = mkReduce(mappedState, knownAggregates, rowA, rowB).fresh

    val (postMap, postMappedState) = mkPostMap(reducedState, knownAggregates, internalRow).fresh

    val (postAggregate, postAggregateState) = mkPostAggregate(query, internalRow, postMappedState).fresh

    val (postFilter, finalState) = mkFilter(postAggregateState, internalRow, query.postFilter).fresh
    assert(finalState.unfinished.isEmpty)

    val defs = finalState.globalDecls.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}" } ++
      finalState.refs.map { case (_, d)                 => q"private val ${d.name}: ${d.tpe} = ${d.value}.asInstanceOf[${d.tpe}]" }

    val tree = q"""
      import _root_.org.yupana.api.Time
      import _root_.org.yupana.api.types.DataType
      import _root_.org.yupana.api.utils.Tokenizer
      import _root_.org.yupana.core.model.InternalRow

      ($params: Array[Any]) =>
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
            $rowA
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

    val finalRequirements = finalState.required -- finalState.index.keySet
    val index = finalRequirements.foldLeft(finalState.index)((i, e) => i + (e -> i.size))

    (tree, index, finalState.refs.map(_._1).toArray)
  }

  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int]) = {
    val tb = currentMirror.mkToolBox()

    val (tree, known, params) = generateCalculator(query, condition)

    logger.whenTraceEnabled {
      val index = known.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      index.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tb.eval(tree).asInstanceOf[Array[Any] => ExpressionCalculator](params), known)
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
      .replaceAll("\\.\\$greater\\$eq", " >= ")
      .replaceAll("\\.\\$greater", " > ")
      .replaceAll("\\.\\$less\\$eq", " <= ")
      .replaceAll("\\.\\$less", " < ")
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
