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

package org.yupana.core.jit.codegen.expressions.regular

import org.threeten.extra.PeriodDuration
import org.yupana.api.Time
import org.yupana.api.query.Expression
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType, TupleDataType }
import org.yupana.core.jit.codegen.CommonGen.mkType
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._
import scala.reflect.{ ClassTag, classTag }

class ConstantExpressionCodeGen(override val expression: Expression[_], v: Any)
    extends ExpressionCodeGen[Expression[_]] {

  private val byRefTypes: Set[ClassTag[_]] = Set(classTag[Time], classTag[BigDecimal], classTag[PeriodDuration])

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valueDeclaration, exprState) = state.withLocalValueDeclaration(expression)

    val (t, ns) = mkValueTree(exprState, expression.dataType)(v)

    val validityTree = q"val ${valueDeclaration.validityFlagName} = true"
    val valueTree = q"val ${valueDeclaration.valueName} = $t"

    CodeGenResult(Seq(validityTree, valueTree), valueDeclaration, ns)
  }

  private def mkValueTree(state: State, dataType: DataType)(v: Any): (Tree, State) = {
    val tpe = mkType(dataType)

    val (t, ns) = dataType.kind match {
      case TypeKind.Regular if byRefTypes.contains(dataType.classTag) =>
        val (name, ns) = state.withRef(v.asInstanceOf[AnyRef], mkType(dataType))
        q"$name" -> ns
      case TypeKind.Regular if byRefTypes.contains(dataType.classTag) =>
        val (name, ns) = state.withRef(v.asInstanceOf[AnyRef], mkType(dataType))
        q"$name" -> ns

      case TypeKind.Regular => Literal(Constant(v)) -> state

      case TypeKind.Tuple =>
        val tt = dataType.asInstanceOf[TupleDataType[_, _]]
        val (a, b) = v.asInstanceOf[(_, _)]
        val (aTree, s1) = mkValueTree(state, tt.aType)(a)
        val (bTree, s2) = mkValueTree(s1, tt.bType)(b)
        (Apply(Ident(TermName("Tuple2")), List(aTree, bTree)), s2)

      case TypeKind.Array =>
        val (c, s) =
          mkSeqValueTree(state, dataType.asInstanceOf[ArrayDataType[_]].valueType, v.asInstanceOf[Iterable[_]])
        val (name, ns) = s.withGlobal(v, mkType(dataType), c)
        q"$name" -> ns
    }
    q"$t.asInstanceOf[$tpe]" -> ns
  }

  private def mkSeqValueTree[T: TypeTag](
      state: State,
      tpe: DataType,
      values: Iterable[T]
  ): (Tree, State) = {
    val (literals, newState) = values.toList.foldLeft((List.empty[Tree], state)) {
      case ((ts, s), v) =>
        val (t, ns) = mkValueTree(s, tpe)(v)
        (ts :+ t, ns)
    }
    Apply(Ident(TermName("Seq")), literals) -> newState
  }
}
