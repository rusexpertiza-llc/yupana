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

package org.yupana.core.sql.parser

sealed trait Condition {
  def simplify: Condition = this
}

case class IsNull(e: SqlExpr) extends Condition
case class IsNotNull(e: SqlExpr) extends Condition

sealed trait Comparison extends Condition {
  val a: SqlExpr
  val b: SqlExpr
}

case class Eq(a: SqlExpr, b: SqlExpr) extends Comparison
case class Ne(a: SqlExpr, b: SqlExpr) extends Comparison
case class Gt(a: SqlExpr, b: SqlExpr) extends Comparison
case class Ge(a: SqlExpr, b: SqlExpr) extends Comparison
case class Lt(a: SqlExpr, b: SqlExpr) extends Comparison
case class Le(a: SqlExpr, b: SqlExpr) extends Comparison

case class And(cs: Seq[Condition]) extends Condition {
  override def simplify: Condition = {
    And(
      cs.flatMap(c =>
        c match {
          case And(ccs) => ccs map (_.simplify)
          case x        => Seq(x)
        }
      )
    )
  }
}

case class Or(cs: Seq[Condition]) extends Condition {
  override def simplify: Condition = {
    Or(
      cs.flatMap(c =>
        c match {
          case Or(ccs) => ccs map (_.simplify)
          case x       => Seq(x)
        }
      )
    )
  }
}

case class In(expr: SqlExpr, values: Seq[Value]) extends Condition
case class NotIn(expr: SqlExpr, values: Seq[Value]) extends Condition

case class ExprCondition(function: SqlExpr) extends Condition

case class BetweenCondition(expr: SqlExpr, from: Value, to: Value) extends Condition

case class Like(a: SqlExpr, pattern: String) extends Condition
