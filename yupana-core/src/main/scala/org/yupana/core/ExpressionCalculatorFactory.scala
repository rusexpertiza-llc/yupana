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
import org.threeten.extra.PeriodDuration
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType, TupleDataType }

import java.sql.Types
import scala.reflect.{ ClassTag, classTag }

trait ExpressionCalculatorFactory {
  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int])
}

object ExpressionCalculatorFactory extends ExpressionCalculatorFactory with StrictLogging with Serializable {
  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  private val tokenizer = TermName("tokenizer")
  private val params = TermName("params")
  private val calculator = q"_root_.org.yupana.core.ExpressionCalculator"

  private val toolBox = currentMirror.mkToolBox()

  private val byRefTypes: Set[ClassTag[_]] = Set(classTag[Time], classTag[BigDecimal], classTag[PeriodDuration])

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
      typeDecls: Map[TypeName, Tree],
      trees: Seq[Tree],
      exprId: Int,
      prefix: String = "",
      parent: Option[State] = None
  ) {
    def withDeclaration(className: TypeName, tree: Tree): State = {
      if (typeDecls.contains(className)) this else this.copy(typeDecls = this.typeDecls + (className -> tree))
    }

    def withRequired(e: Expression[_]): State = copy(required = required + e).withExpr(e)

    def withExpr(e: Expression[_]): State = {
      if (index.contains(e)) this else this.copy(index = this.index + (e -> index.size))
    }

    def findLocal(e: Expression[_]): Option[LocalDecl] = {
      localDecls.find(_.e == e) orElse parent.flatMap(_.findLocal(e))
    }

    def withDefine(row: TermName, e: Expression[_], v: Tree): State = {
      if (required.contains(e)) {
        val newState = withExpr(e)
        val idx = newState.index(e)
        newState.copy(trees = q"$row.set($idx, $v)" +: trees)
      } else if (findLocal(e).isEmpty) {
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
      } else if (findLocal(e).isEmpty) {
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
        case None                        => withNewRef(ref, tpe)
      }
    }

    def withNewRef(ref: AnyRef, tpe: Tree): (TermName, State) = {
      val name = TermName(s"e_$exprId")
      val refId = refs.size
      val tree = q"$params($refId)"
      val ns = copy(refs = refs :+ (ref -> Decl(name, tpe, tree)), exprId = exprId + 1)
      name -> ns
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

    def render: Tree = {
      val locals = localDecls.reverse.flatMap {
        case decl @ LocalDecl(e, n, v, d) =>
          val tpe = mkType(e)
          val defTree = d.map(t => q"val ${decl.defName}: Boolean = $t")
          val default = mkDefault(e.dataType)
          val tree = if (defTree.isDefined) q"if (${decl.defName}) $v else $default.asInstanceOf[$tpe]" else v

          Seq(defTree, Some(q"val $n: $tpe = $tree")).flatten
      }

      q"""
        ..$locals
        ..${trees.reverse}
      """
    }

    def fresh: (Tree, State) = {
      render -> State(index, required, unfinished, refs, globalDecls, Seq.empty, typeDecls, Seq.empty, exprId)
    }

    def nested(prefix: String): State = State(
      index,
      required,
      unfinished,
      refs,
      globalDecls,
      Seq.empty,
      Map.empty,
      Seq.empty,
      exprId,
      prefix + this.prefix,
      Some(this)
    )

    def finishNested: (Tree, State) = {
      parent match {
        case Some(p) =>
          render -> copy(localDecls = p.localDecls, trees = p.trees, prefix = p.prefix, parent = p.parent)

        case None => throw new IllegalStateException("Merge called, but parent is not defined")
      }
    }

    private def nextLocalName: TermName = TermName(s"l${prefix}_${localDecls.size}")
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

    tpe.kind match {
      case TypeKind.Regular if byRefTypes.contains(tpe.classTag) =>
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
          mkSeqValue(state, tpe.asInstanceOf[ArrayDataType[_]].valueType, v.asInstanceOf[Iterable[_]])
        val (name, ns) = s.withGlobal(v, mkType(tpe), c)
        q"$name" -> ns
    }
  }

  private def mkGet(state: State, row: TermName, e: Expression[_], tpe: Tree): Option[(Tree, State)] = {
    e match {
      case x if state.unfinished.contains(x) => None

      case ConstantExpr(x, false) =>
        val (v, ns) = mapValue(state, e.dataType)(x)
        Some(q"$v.asInstanceOf[$tpe]" -> ns)

      case ConstantExpr(v, true) =>
        val (name, ns) = state.withNewRef(v.asInstanceOf[AnyRef], mkType(e.dataType))
        Some(q"$name" -> ns)

      case NullExpr(_) =>
        val (v, ns) = mapValue(state, e.dataType)(null)
        Some(q"$v.asInstanceOf[$tpe]" -> ns)

      case TrueExpr =>
        val (v, ns) = mapValue(state, e.dataType)(true)
        Some(q"$v" -> ns)

      case FalseExpr =>
        val (v, ns) = mapValue(state, e.dataType)(false)
        Some(q"$v" -> ns)

      case ae @ ArrayExpr(exprs) if e.kind == Const =>
        val (lits, newState) = exprs.foldLeft((Seq.empty[Tree], state)) {
          case ((ts, s), ConstantExpr(v, _)) =>
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
        state
          .findLocal(x)
          .map(x => q"${x.name}" -> state)
    }
  }

  private def mkGet(state: State, row: TermName, e: Expression[_]): Option[(Tree, State)] = {
    mkGet(state, row, e, mkType(e))
  }

  private def mkGetNow(state: State, row: TermName, e: Expression[_], tpe: Tree): (Tree, State) = {
    mkGet(state, row, e, tpe) getOrElse (throw new IllegalStateException(s"Unknown expression $e"))
  }

  private def mkGetNow(state: State, row: TermName, e: Expression[_]): (Tree, State) = {
    mkGet(state, row, e) getOrElse (throw new IllegalStateException(s"Unknown expression $e"))
  }

  private def mkIsDefined(state: State, row: TermName, e: Expression[_]): Option[Tree] = {
    e match {
      case ConstantExpr(_, _)                                                   => None
      case NullExpr(_)                                                          => Some(q"false")
      case TimeExpr                                                             => None
      case DimensionExpr(_)                                                     => None
      case CountExpr(_)                                                         => None
      case DistinctCountExpr(_)                                                 => None
      case DistinctRandomExpr(_)                                                => None
      case AvgExpr(_)                                                           => None
      case HLLCountExpr(_, _)                                                   => None
      case e: AggregateExpr[_, _, _] if mkIsDefined(state, row, e.expr).isEmpty => None

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
          .orElse(state.findLocal(e).flatMap(_.isDefined))
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

  private val truncTime = q"_root_.org.yupana.core.ExpressionCalculator.truncateTime"
  private val truncTimeBy = q"_root_.org.yupana.core.ExpressionCalculator.truncateTimeBy"
  private val monday = q"_root_.java.time.DayOfWeek.MONDAY"
  private val cru = q"_root_.java.time.temporal.ChronoUnit"
  private val adj = q"_root_.java.time.temporal.TemporalAdjusters"

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

  private def mkInner(prefix: String, state: State, row: TermName, e: Expression[_]): (Tree, State) = {
    val s = mkSet(state.nested(prefix), row, e)
    val (t, getEState) = mkGetNow(s, row, e)
    val (tree, updatedState) = getEState.finishNested
    val result = q"""..$tree
       $t"""

    result -> updatedState
  }

  private def mkSetCondition(state: State, row: TermName, c: ConditionExpr[_]): State = {
    val newState = mkSet(state, row, c.condition)
    mkGet(newState, row, c.condition) match {
      case Some((getIf, ifState)) =>
        val (getThen, thenState) = mkInner("t", ifState, row, c.positive)
        val (getElse, elseState) = mkInner("e", thenState, row, c.negative)
        val thenDef = mkIsDefined(elseState, row, c.positive)
        val elseDef = mkIsDefined(elseState, row, c.negative)
        val readCondition = (thenDef, elseDef) match {
          case (Some(td), Some(ed)) => Some(q"($getIf && $td) || (!$getIf && $ed)")
          case (Some(td), None)     => Some(q"!$getIf || $td")
          case (None, Some(ed))     => Some(q"$getIf || $ed")
          case (None, None)         => None
        }
        readCondition match {
          case Some(cond) => elseState.withDefineIf(row, c, cond, q"if ($getIf) $getThen else $getElse")
          case None       => elseState.withDefine(row, c, q"if ($getIf) $getThen else $getElse")
        }

      case None =>
        val thenState = mkSet(newState, row, c.positive)
        val elseState = mkSet(thenState, row, c.negative)
        elseState.withUnfinished(c)
    }
  }

  private def mkSet(state: State, row: TermName, e: Expression[_]): State = {

    if (state.index.contains(e)) {
      state
    } else {

      e match {
        case ConstantExpr(c, _) =>
          val (v, s) = mapValue(state, e.dataType)(c)
          if (s.required.contains(e)) s.withDefine(row, e, v) else s
        case TrueExpr =>
          val (v, s) = mapValue(state, e.dataType)(true)
          if (s.required.contains(e)) s.withDefine(row, e, v) else s
        case FalseExpr =>
          val (v, s) = mapValue(state, e.dataType)(false)
          if (s.required.contains(e)) s.withDefine(row, e, v) else s
        case NullExpr(_) =>
          val (v, s) = mapValue(state, e.dataType)(null)
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

        case Double2BigDecimalExpr(a) => mkSetUnary(state, row, e, a, d => q"BigDecimal($d)")

        case Long2BigDecimalExpr(a) => mkSetUnary(state, row, e, a, l => q"BigDecimal($l)")
        case Long2DoubleExpr(a)     => mkSetUnary(state, row, e, a, l => q"$l.toDouble")

        case Int2BigDecimalExpr(a) => mkSetUnary(state, row, e, a, i => q"BigDecimal($i)")
        case Int2DoubleExpr(a)     => mkSetUnary(state, row, e, a, i => q"$i.toDouble")
        case Int2LongExpr(a)       => mkSetUnary(state, row, e, a, i => q"$i.toLong")

        case Short2BigDecimalExpr(a) => mkSetUnary(state, row, e, a, s => q"BigDecimal($s)")
        case Short2DoubleExpr(a)     => mkSetUnary(state, row, e, a, s => q"$s.toDouble")
        case Short2LongExpr(a)       => mkSetUnary(state, row, e, a, s => q"$s.toLong")
        case Short2IntExpr(a)        => mkSetUnary(state, row, e, a, s => q"$s.toInt")

        case Byte2BigDecimalExpr(a) => mkSetUnary(state, row, e, a, b => q"BigDecimal($b)")
        case Byte2DoubleExpr(a)     => mkSetUnary(state, row, e, a, b => q"$b.toDouble")
        case Byte2LongExpr(a)       => mkSetUnary(state, row, e, a, b => q"$b.toLong")
        case Byte2IntExpr(a)        => mkSetUnary(state, row, e, a, b => q"$b.toInt")
        case Byte2ShortExpr(a)      => mkSetUnary(state, row, e, a, b => q"$b.toShort")

        case ToStringExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toString")

        case TruncYearExpr(a) => mkSetUnary(state, row, e, a, x => q"$truncTime($adj.firstDayOfYear)($x)")
        case TruncQuarterExpr(a) =>
          mkSetUnary(
            state,
            row,
            e,
            a,
            x =>
              q"$truncTimeBy(dTime => dTime.`with`($adj.firstDayOfMonth).withMonth(dTime.getMonth.firstMonthOfQuarter.getValue))($x)"
          )
        case TruncMonthExpr(a)  => mkSetUnary(state, row, e, a, x => q"$truncTime($adj.firstDayOfMonth)($x)")
        case TruncWeekExpr(a)   => mkSetUnary(state, row, e, a, x => q"$truncTime($adj.previousOrSame($monday))($x)")
        case TruncDayExpr(a)    => mkSetUnary(state, row, e, a, x => q"$truncTime($cru.DAYS)($x)")
        case TruncHourExpr(a)   => mkSetUnary(state, row, e, a, x => q"$truncTime($cru.HOURS)($x)")
        case TruncMinuteExpr(a) => mkSetUnary(state, row, e, a, x => q"$truncTime($cru.MINUTES)($x)")
        case TruncSecondExpr(a) => mkSetUnary(state, row, e, a, x => q"$truncTime($cru.SECONDS)($x)")

        case ExtractYearExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getYear")
        case ExtractQuarterExpr(a) =>
          mkSetUnary(state, row, e, a, x => q"1 + ($x.toLocalDateTime.getMonth.getValue - 1) / 3")
        case ExtractMonthExpr(a)  => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getMonthValue")
        case ExtractDayExpr(a)    => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getDayOfMonth")
        case ExtractHourExpr(a)   => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getHour")
        case ExtractMinuteExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getMinute")
        case ExtractSecondExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLocalDateTime.getSecond")

        case TimeMinusExpr(a, b) =>
          mkSetBinary(state, row, e, a, b, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)")
        case TimeMinusPeriodExpr(a, b)  => mkSetBinary(state, row, e, a, b, (t, p) => q"Time($t.toDateTime.minus($p))")
        case TimePlusPeriodExpr(a, b)   => mkSetBinary(state, row, e, a, b, (t, p) => q"Time($t.toDateTime.plus($p))")
        case PeriodPlusPeriodExpr(a, b) => mkSetBinary(state, row, e, a, b, (x, y) => q"$x plus $y")

        case IsNullExpr(a)    => mkSetUnary(state, row, e, a, _ => q"false", Some(q"true"))
        case IsNotNullExpr(a) => mkSetUnary(state, row, e, a, _ => q"true", Some(q"false"))

        case LowerExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toLowerCase")
        case UpperExpr(a) => mkSetUnary(state, row, e, a, x => q"$x.toUpperCase")

        case c @ ConditionExpr(_, _, _) => mkSetCondition(state, row, c)

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
      case Some(ConstantExpr(v, _)) => state.appendLocal(q"$v")
      case Some(TrueExpr)           => state.appendLocal(q"true")
      case Some(FalseExpr)          => state.appendLocal(q"false")

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

  private def avgClassName(dataType: DataType): TypeName = TypeName(s"Avg_${dataType.meta.sqlTypeName}")

  private def mkAvg(dataType: DataType, state: State): State = {
    val tpe = mkType(dataType)
    val accTpe = if (Set("Short", "Byte").contains(className(dataType))) {
      Ident(TypeName("Long"))
    } else {
      tpe
    }

    val cn = avgClassName(dataType)

    val tree = q"""
       class $cn {
       
         var a: $accTpe = 0
         var c: Int = 0
         
         def prepare(initial: $tpe): $cn = {
             a = initial
             c = 1
             this
         }

         def +(x: $tpe): $cn = {
             a += x
             c += 1
             this
         }

         def ++(that: $cn): $cn = {
           this.a += that.a
           this.c += that.c
           this
         }

         def result: BigDecimal = if (c != 0) { BigDecimal(a.toDouble / c) } else  { null }
       }
     """
    state.withDeclaration(cn, tree)
  }

  private def mkZero(
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

        case AvgExpr(_) =>
          val avgClass = avgClassName(ae.expr.dataType)
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) =>
              mkAvg(ae.expr.dataType, s).withDefine(
                row,
                ae,
                q"""
                 val avg = new $avgClass()
                 if ($d) {
                  avg.prepare($exprValue)
                 } else {
                  avg
                 }
                """
              )
            case None =>
              mkAvg(ae.expr.dataType, s).withDefine(
                row,
                ae,
                q"""
                  val avg = new $avgClass()
                  avg.prepare($exprValue)
                """
              )
          }

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
        case HLLCountExpr(_, b) =>
          val valTpe = mkType(ae.expr)
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) =>
              s.withDefine(
                row,
                ae,
                q"""
                val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$valTpe]($b)
                if ($d) {
                  agg.prepare($exprValue)
                } else {
                  agg.monoid.empty
                }
              """
              )
            case None =>
              s.withDefine(
                row,
                ae,
                q"""
                val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$valTpe]($b)
                agg.prepare($exprValue)
              """
              )
          }
      }
    }
  }

  private def mkFold(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      acc: TermName,
      row: TermName
  ): State = {

    aggregates.foldLeft(state) { (s, ae) =>
      ae match {
        case SumExpr(_) => mkSetFold(s, acc, row, ae, identity, None, (a, r) => q"$a + $r")

        case MinExpr(_) =>
          mkSetFold(s, acc, row, ae, identity, None, (a, r) => q"${ordValName(ae.expr.dataType)}.min($a, $r)")

        case MaxExpr(_) =>
          mkSetFold(s, acc, row, ae, identity, None, (a, r) => q"${ordValName(ae.expr.dataType)}.max($a, $r)")

        case AvgExpr(_) =>
          mkSetFold(
            s,
            acc,
            tq"${avgClassName(ae.expr.dataType)}",
            row,
            ae,
            identity,
            None,
            (a, r) => q"$a + ($r)"
          )

        case CountExpr(_) =>
          mkSetFold(s, acc, row, ae, _ => q"1L", Some(q"0L"), (a, r) => q"$a + $r")
        case DistinctCountExpr(_) | DistinctRandomExpr(_) =>
          val valTpe = mkType(ae.expr)
          mkSetFold(s, acc, tq"Set[$valTpe]", row, ae, identity, None, (a, r) => q"$a + $r")
        case HLLCountExpr(_, e) =>
          val valTpe = mkType(ae.expr)
          mkSetFold(
            s,
            acc,
            tq"_root_.com.twitter.algebird.HLL",
            row,
            ae,
            identity,
            None,
            (a, r) => q"""
                val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$valTpe]($e)
                agg.append($a, $r)
              """
          )
      }
    }
  }

  private def mkSetFold(
      state: State,
      acc: TermName,
      row: TermName,
      ae: AggregateExpr[_, _, _],
      map: Tree => Tree,
      default: Option[Tree],
      fold: (Tree, Tree) => Tree
  ): State = {
    mkSetFold(state, acc, mkType(ae), row, ae, map, default, fold)
  }

  private def mkSetFold(
      state: State,
      acc: TermName,
      accType: Tree,
      row: TermName,
      ae: AggregateExpr[_, _, _],
      map: Tree => Tree,
      default: Option[Tree],
      fold: (Tree, Tree) => Tree
  ): State = {
    val (aValue, s1) = mkGetNow(state, acc, ae, accType)
    val s2 = mkSet(s1, row, ae.expr)
    val (rValue, s3) = mkGetNow(s2, row, ae.expr)

    (mkIsDefined(s3, acc, ae), mkIsDefined(s3, row, ae.expr), default) match {
      case (Some(ad), Some(rd), Some(d)) =>
        val s4 = s3.withDefine(row, ae.expr, q"if ($rd) ${map(rValue)} else $d")
        val (rOrDefault, s5) = mkGetNow(s4, row, ae.expr, accType)
        s5.withDefine(acc, ae, q"if ($ad) ${fold(aValue, rOrDefault)} else $rOrDefault")

      case (Some(ad), Some(rd), None) =>
        s3.withDefineIf(acc, ae, rd, q"if ($ad) ${fold(aValue, map(rValue))} else ${map(rValue)}")

      case (None, Some(rd), Some(d)) => s3.withDefine(acc, ae, fold(aValue, q"if ($rd) ${map(rValue)} else $d"))
      case (None, Some(rd), None)    => s3.withDefineIf(acc, ae, rd, fold(aValue, map(rValue)))

      case (Some(ad), None, _) => s3.withDefine(acc, ae, q"if ($ad) ${fold(aValue, map(rValue))} else ${map(rValue)}")
      case (None, None, _)     => s3.withDefine(acc, ae, fold(aValue, map(rValue)))
    }
  }
  private def mkSetReduce(
      state: State,
      rowA: TermName,
      rowB: TermName,
      ae: AggregateExpr[_, _, _],
      f: (Tree, Tree) => Tree
  ): State = {
    mkSetReduce(state, rowA, rowB, mkType(ae), ae, f)
  }

  private def mkSetReduce(
      state: State,
      rowA: TermName,
      rowB: TermName,
      tpe: Tree,
      ae: AggregateExpr[_, _, _],
      f: (Tree, Tree) => Tree
  ): State = {
    val (aValue, s1) = mkGetNow(state, rowA, ae, tpe)
    val (bValue, s2) = mkGetNow(s1, rowB, ae, tpe)

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

  private def mkCombine(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      rowA: TermName,
      rowB: TermName
  ): State = {

    aggregates.foldLeft(state) { (s, ae) =>
      val valueTpe = mkType(ae.expr)
      ae match {
        case SumExpr(_) => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"$a + $b")
        case MinExpr(_) => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"${ordValName(ae.expr.dataType)}.min($a, $b)")
        case MaxExpr(_) => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"${ordValName(ae.expr.dataType)}.max($a, $b)")
        case AvgExpr(_) =>
          mkSetReduce(
            s,
            rowA,
            rowB,
            tq"${avgClassName(ae.expr.dataType)}",
            ae,
            (a, b) => q"$a ++ $b"
          )
        case CountExpr(_) => mkSetReduce(s, rowA, rowB, ae, (a, b) => q"$a + $b")
        case DistinctCountExpr(_) | DistinctRandomExpr(_) =>
          mkSetReduce(s, rowA, rowB, tq"Set[$valueTpe]", ae, (a, b) => q"$a ++ $b")
        case HLLCountExpr(_, e) =>
          mkSetReduce(
            s,
            rowA,
            rowB,
            tq"_root_.com.twitter.algebird.HLL",
            ae,
            (a, b) => q"""
                val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$valueTpe]($e)
                val hll = agg.monoid
                hll.combine($a, $b)
               """
          )
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
        case AvgExpr(_)           => Some(q"$row.get[${avgClassName(ae.expr.dataType)}]($idx).result" -> s)
        case CountExpr(_)         => None
        case DistinctCountExpr(_) => Some(q"$row.get[Set[$valueTpe]]($idx).size" -> s)
        case HLLCountExpr(_, _) =>
          Some(
            q"""
              $row.get[_root_.com.twitter.algebird.HLL]($idx).approximateSize.estimate
            """ -> s
          )
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
      values: Iterable[T]
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
        Map.empty,
        Seq.empty,
        0
      )

    val (filter, filteredState) = mkFilter(initialState, internalRow, condition).fresh

    val (evaluate, evaluatedState) = mkEvaluate(query, internalRow, filteredState).fresh

    val knownAggregates = (evaluatedState.index.keySet ++ evaluatedState.required).collect {
      case ae: AggregateExpr[_, _, _] => ae
    }.toSeq

    val beforeAggregationState = evaluatedState.copy(unfinished = Set.empty)

    val acc = TermName("acc")

    val (map, mappedState) = mkZero(beforeAggregationState, knownAggregates, internalRow).fresh

    val (fold, foldedState) = mkFold(mappedState, knownAggregates, acc, internalRow).fresh

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val (reduce, reducedState) = mkCombine(foldedState, knownAggregates, rowA, rowB).fresh

    val (postMap, postMappedState) = mkPostMap(reducedState, knownAggregates, internalRow).fresh

    val (postAggregate, postAggregateState) = mkPostAggregate(query, internalRow, postMappedState).fresh

    val (postFilter, finalState) = mkFilter(postAggregateState, internalRow, query.postFilter).fresh
    assert(finalState.unfinished.isEmpty)

    val defs = finalState.globalDecls.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}" } ++
      finalState.refs.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}.asInstanceOf[${d.tpe}]" }

    val typeDecls = finalState.typeDecls.values

    val tree = q"""
      import _root_.org.yupana.api.Time
      import _root_.org.yupana.api.types.DataType
      import _root_.org.yupana.api.utils.Tokenizer
      import _root_.org.yupana.core.model.InternalRow
      import _root_.org.threeten.extra.PeriodDuration
      import _root_.org.threeten.extra.PeriodDuration
      import _root_.org.yupana.core.utils.Hash128Utils.timeHash

      ..$typeDecls

      ($params: Array[Any]) =>
        new _root_.org.yupana.core.ExpressionCalculator {
          ..$defs
        
          override def evaluateFilter($tokenizer: Tokenizer, $internalRow: InternalRow): Boolean = $filter
          
          override def evaluateExpressions($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $evaluate
            $internalRow
          }
          
          override def evaluateZero($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $map
            $internalRow
          }
          
          override def evaluateSequence($tokenizer: Tokenizer, $acc: InternalRow, $internalRow: InternalRow): InternalRow = {
            $fold
            $acc
          }
          
          override def evaluateCombine($tokenizer: Tokenizer, $rowA: InternalRow, $rowB: InternalRow): InternalRow = {
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
    val paramsArray = finalState.refs.map(_._1).toArray[Any]

    logger.whenTraceEnabled {
      val sortedIndex = index.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      sortedIndex.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Params size: ${paramsArray.length}")
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tree, index, paramsArray)
  }

  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int]) = {
    val (tree, known, params) = generateCalculator(query, condition)

    val res = compile(tree)(params)

    (res, known)
  }

  def compile(tree: Tree): Array[Any] => ExpressionCalculator = {
    toolBox.eval(tree).asInstanceOf[Array[Any] => ExpressionCalculator]
  }

  private def prettyTree(tree: Tree): String = {
    show(tree)
      .replaceAll("_root_\\.([a-z_]+\\.)+", "")
      .replaceAll("\\.\\$bang\\$eq", " != ")
      .replaceAll("\\.\\$eq\\$eq", " == ")
      .replaceAll("\\.\\$amp\\$amp", " && ")
      .replaceAll("\\.\\$bar\\$bar", " || ")
      .replaceAll("\\.\\$plus\\$plus", " ++ ")
      .replaceAll("\\$plus\\$plus", "++")
      .replaceAll("\\.\\$plus\\$eq", " += ")
      .replaceAll("\\.\\$plus", " + ")
      .replaceAll("\\$plus", "+")
      .replaceAll("\\.\\$minus", " - ")
      .replaceAll("\\.\\$div", " / ")
      .replaceAll("\\.\\$greater\\$eq", " >= ")
      .replaceAll("\\.\\$greater", " > ")
      .replaceAll("\\.\\$less\\$eq", " <= ")
      .replaceAll("\\.\\$less", " < ")
  }
}
