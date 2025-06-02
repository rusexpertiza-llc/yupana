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

package org.yupana.core.jit.codegen.stages

import org.yupana.api.query.Expression.Condition
import org.yupana.core.jit.{ ExpressionCodeGenFactory, State }

import scala.reflect.runtime.universe._

object FilterStageGen {
  def mkFilter(state: State, row: TermName, condition: Option[Condition]): (Seq[Tree], State) = {
    condition match {
      case None       => Seq(q"true") -> state
      case Some(cond) =>
        val res = ExpressionCodeGenFactory.codeGenerator(cond).generateEvalCode(state, row)
        val tree = q"${res.valueDeclaration.validityFlagName} && ${res.valueDeclaration.valueName}"
        (res.trees :+ tree) -> res.state
    }
  }
}
