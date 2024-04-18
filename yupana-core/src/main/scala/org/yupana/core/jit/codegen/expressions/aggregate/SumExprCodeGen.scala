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

package org.yupana.core.jit.codegen.expressions.aggregate

import org.yupana.api.query.SumExpr
import org.yupana.core.jit.ValueDeclaration

import scala.reflect.runtime.universe._

class SumExprCodeGen(expr: SumExpr[_, _]) extends SimpleAggExprCodeGen[SumExpr[_, _]] {

  override def expression: SumExpr[_, _] = expr

  override def zeroTree(innerExprValue: ValueDeclaration): Tree = q"${innerExprValue.valueName}"

  override def foldTree(accValue: ValueDeclaration, innerExprValue: ValueDeclaration): Tree =
    q"${accValue.valueName} + ${innerExprValue.valueName}"

  override def combineTree(accValueA: ValueDeclaration, accValueB: ValueDeclaration): Tree =
    q"${accValueA.valueName} + ${accValueB.valueName}"

}
