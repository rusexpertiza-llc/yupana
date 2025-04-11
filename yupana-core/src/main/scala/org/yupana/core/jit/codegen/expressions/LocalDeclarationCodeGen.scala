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

package org.yupana.core.jit.codegen.expressions

import org.yupana.api.query.Expression
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe

class LocalDeclarationCodeGen(override val expression: Expression[_], codeGen: ExpressionCodeGen[_])
    extends ExpressionCodeGen[Expression[_]] {

  override def generateEvalCode(state: State, row: universe.TermName): CodeGenResult = {
    if (state.hasLocalValueDeclaration(expression)) {
      CodeGenResult(Seq.empty, state.getLocalValueDeclaration(expression), state)
    } else {
      codeGen.generateEvalCode(state, row)
    }
  }
}
