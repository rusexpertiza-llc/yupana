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

sealed trait SqlExpr {
  def proposedName: Option[String]
}

case class Constant(value: Value) extends SqlExpr {
  override def proposedName: Option[String] = Some(value.asString)
}

case class SqlArray(values: Seq[Value]) extends SqlExpr {
  override def proposedName: Option[String] = None
}

case class FieldName(name: String) extends SqlExpr {
  val lowerName: String = name.toLowerCase
  override val proposedName: Option[String] = Some(name)
}

case class FunctionCall(function: String, exprs: List[SqlExpr]) extends SqlExpr {
  override def proposedName: Option[String] = {
    val exprsNames = exprs.flatMap(_.proposedName)
    if (exprsNames.size == exprs.size) {
      val args = exprsNames.mkString(",")
      Some(s"$function($args)")
    } else None
  }
}

case class Case(conditionalValues: Seq[(Condition, SqlExpr)], defaultValue: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = None
}

case class Plus(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] =
    for {
      aName <- a.proposedName
      bName <- b.proposedName
    } yield s"${aName}_plus_$bName"
}

case class Minus(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] =
    for {
      aName <- a.proposedName
      bName <- b.proposedName
    } yield s"${aName}_minus_$bName"
}

case class Multiply(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] =
    for {
      aName <- a.proposedName
      bName <- b.proposedName
    } yield s"${aName}_mul_$bName"
}

case class Divide(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] =
    for {
      aName <- a.proposedName
      bName <- b.proposedName
    } yield s"${aName}_div_$bName"
}

case class UMinus(x: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = x.proposedName.map(n => s"minus_$n")
}

case class CastExpr(e: SqlExpr, resultType: String) extends SqlExpr {
  override def proposedName: Option[String] = e.proposedName.map(n => s"${n}_as_$resultType")
}
