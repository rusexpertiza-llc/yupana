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
import org.yupana.core.jit.{CodeGenResult, State}

import java.sql.Types
import scala.reflect.runtime.universe._

class DivFracExpressionCodeGen(override val expression: DivExpr[_]) extends ExpressionCodeGen[DivExpr[_]] {

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    if (expression.dataType.meta.sqlType != Types.DECIMAL) {
      BinaryExpressionCodeGen(expression, (x, y) => q"$x / $y").generateEvalCode(state, row)
    } else {
      val scale = expression.dataType.meta.scale
      BinaryExpressionCodeGen(
        expression,
        (x, y) =>
          q"new BigDecimal($x.bigDecimal.divide($y.bigDecimal, $scale, _root_.java.math.RoundingMode.HALF_EVEN))"
      ).generateEvalCode(state, row)
    }
  }
}
