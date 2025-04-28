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

import org.yupana.api.query.PlusExpr
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.codegen.expressions.regular.CodegenUtils.{ isCurrency, isTime }
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._

class PlusExpressionCodeGen(override val expression: PlusExpr[_, _, _]) extends ExpressionCodeGen[PlusExpr[_, _, _]] {
  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val f: (Tree, Tree) => Tree = {
      if (isCurrency(expression.a)) { (x, y) =>
        q"Currency($x.value + $y.value)"
      } else if (isTime(expression.a)) { (t, p) =>
        q"Time($t.toDateTime.plus($p))"
      } else { (x, y) =>
        q"$x + $y"
      }
    }

    BinaryExpressionCodeGen(expression, f).generateEvalCode(state, row)
  }
}
