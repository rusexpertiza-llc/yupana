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

import org.yupana.api.query.DivExpr
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.codegen.expressions.regular.CodegenUtils.{ isCurrency, isDecimal, isDouble }
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._

class DivExpressionCodeGen(override val expression: DivExpr[_, _, _]) extends ExpressionCodeGen[DivExpr[_, _, _]] {

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val f: (Tree, Tree) => Tree = if (isDecimal(expression.a)) {
      val scale = expression.a.dataType.meta.scale
      if (isDecimal(expression.b)) { (x, y) =>
        q"new BigDecimal($x.bigDecimal.divide($y.bigDecimal, $scale, _root_.java.math.RoundingMode.HALF_EVEN))"
      } else { (x, y) =>
        q"new BigDecimal($x.bigDecimal.divide(_root_.java.math.BigDecimal.valueOf($y), $scale, _root_.java.math.RoundingMode.HALF_EVEN))"
      }
    } else if (isCurrency(expression.a)) {
      if (isDouble(expression.b)) { (x, y) =>
        q"Currency(($x.value.toDouble / $y).toLong)"
      } else { (x, y) =>
        q"Currency($x.value / $y)"
      }

    } else {
      if (!isDecimal(expression.b)) { (x, y) =>
        q"$x / $y"
      } else {
        val scale = expression.a.dataType.meta.scale
        (x, y) =>
          q"new BigDecimal(_root_.java.math.BigDecimal.valueOf($x).divide($y.bigDecimal, $scale, _root_.java.math.RoundingMode.HALF_EVEN))"
      }
    }
    BinaryExpressionCodeGen(expression, f).generateEvalCode(state, row)
  }
}
