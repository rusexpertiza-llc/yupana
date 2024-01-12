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

package org.yupana.core.jit.codegen

import org.yupana.api.query.{ AggregateExpr, Expression, Query, QueryField, WindowFunctionExpr }
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType, TupleDataType }
import org.yupana.core.jit.{ State, ValueDeclaration }

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object CommonGen {
  def initVal(expression: Expression[_]): Tree = {
    val dt: DataType = expression.dataType
    dt.classTag.runtimeClass match {
      case Classes.LongClass       => q"0L"
      case Classes.IntClass        => q"0"
      case Classes.ShortClass      => q"0.toShort"
      case Classes.ByteClass       => q"0.toByte"
      case Classes.DoubleClass     => q"0.0"
      case Classes.FloatClass      => q"0.0f"
      case Classes.BooleanClass    => q"false"
      case Classes.BigDecimalClass => q"BigDecimal(0)"
      case _                       => q"null"
    }
  }

  def ordValName(dt: DataType): TermName = {
    TermName(s"ord_${dt.toString}")
  }

  def mkType(e: Expression[_]): Tree = {
    mkType(e.dataType)
  }
  def mkType(dataType: DataType): Tree = {
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

  def className(classTag: ClassTag[_]): String = {
    val tpe = classTag.toString()
    val lastDot = tpe.lastIndexOf(".")
    tpe.substring(lastDot + 1)
  }
  def className(dataType: DataType): String = {
    className(dataType.classTag)
  }

  def avgClassName(dataType: DataType): TypeName = {
    TypeName(s"Avg_${dataType.meta.sqlTypeName}")
  }

  def findAggregates(fields: Seq[QueryField]): Seq[AggregateExpr[_, _, _]] = {
    collectExprs(fields) {
      case e: AggregateExpr[_, _, _] => e
    }
  }

  def findWindowFunctions(fields: Seq[QueryField]): Seq[WindowFunctionExpr[_, _]] = {
    collectExprs(fields) {
      case e: WindowFunctionExpr[_, _] => e
    }
  }

  def collectExprs[T](fields: Seq[QueryField])(pf: PartialFunction[Expression[_], T]): Seq[T] = {
    fields.flatMap(f => collectExprs[T](f.expr)(pf)).distinct
  }

  def collectExprs[T](expr: Expression[_])(pf: PartialFunction[Expression[_], T]): Seq[T] = {
    expr
      .fold(Seq.empty[T]) { (aggs, e) =>
        pf.andThen(t => aggs :+ t).applyOrElse(e, (_: Expression[_]) => aggs)
      }
      .distinct
  }

  def copyGroupByFields(state: State, query: Query, row: TermName): State = {
    query.groupBy.zipWithIndex.foldLeft(state) {
      case (accState, (expr, idx)) =>
        val valName = ValueDeclaration(s"group_$idx")
        accState
          .withReadFromRow(row, expr, valName)
          .withWriteToRow(row, expr, valName)
    }
  }

  object Classes {
    val LongClass = classOf[Long]
    val IntClass = classOf[Int]
    val ShortClass = classOf[Short]
    val ByteClass = classOf[Byte]
    val DoubleClass = classOf[Double]
    val FloatClass = classOf[Float]
    val BooleanClass = classOf[Boolean]
    val BigDecimalClass = classOf[BigDecimal]
  }
}
