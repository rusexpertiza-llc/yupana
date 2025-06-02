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

package org.yupana.core.jit

import org.yupana.api.query.Expression
import org.yupana.core.jit.State.RowOperation

import scala.reflect.runtime.universe._

case class State(
    valueExprIndex: Map[Expression[_], Int],
    refExprIndex: Map[Expression[_], Int],
    refs: Seq[(AnyRef, Decl)],
    globalDecls: Seq[(Any, Decl)],
    rowOperations: Seq[RowOperation],
    localValueDeclarations: Map[Expression[_], ValueDeclaration]
) {

  def fresh(): State = {
    this.copy(rowOperations = Seq.empty, localValueDeclarations = Map.empty)
  }
  def withLocalValues(from: State): State = {
    this.copy(localValueDeclarations = from.localValueDeclarations)
  }

  def expressionIndex(expression: Expression[_]): Int = {
    valueExprIndex.getOrElse(expression, refExprIndex(expression))
  }

  def hasWriteOps: Boolean = {
    rowOperations.exists { op =>
      op.opType == State.RowOpType.WriteRef || op.opType == State.RowOpType.WriteField
    }
  }

  def withLocalValueDeclaration(expr: Expression[_]): (ValueDeclaration, State) = {
    val s1 = withExpression(expr)
    val i = s1.expressionIndex(expr)
    val decl = ValueDeclaration(s"expr_$i")
    decl -> s1.copy(localValueDeclarations = localValueDeclarations + (expr -> decl))
  }

  def hasLocalValueDeclaration(expr: Expression[_]): Boolean = {
    localValueDeclarations.contains(expr)
  }

  def getLocalValueDeclaration(expr: Expression[_]): ValueDeclaration = {
    localValueDeclarations(expr)
  }

  def withExpression(expr: Expression[_]): State = {
    if (expr.dataType.internalStorable.isRefType) {
      withRefExpression(expr)
    } else {
      withValueExpression(expr)
    }
  }

  def withValueExpression(expr: Expression[_]): State = {
    this.copy(valueExprIndex =
      if (valueExprIndex.contains(expr)) valueExprIndex
      else valueExprIndex + (expr -> (valueExprIndex.size + refExprIndex.size))
    )
  }

  def withRefExpression(expr: Expression[_]): State = {
    this.copy(refExprIndex =
      if (refExprIndex.contains(expr)) refExprIndex
      else refExprIndex + (expr -> (refExprIndex.size + valueExprIndex.size))
    )
  }

  def withReadRefFromRow(row: TermName, expr: Expression[_], tpe: Tree): CodeGenResult = {
    withRowOperation(row, expr, State.RowOpType.ReadRef, Some(tpe))
  }

  def withWriteRefToRow(row: TermName, expr: Expression[_], valueDeclaration: ValueDeclaration): State = {
    withRowOperation(row, expr, State.RowOpType.WriteRef, valueDeclaration, None)
  }

  def withWriteRefToRow(row: TermName, expr: Expression[_]): CodeGenResult = {
    withRowOperation(row, expr, State.RowOpType.WriteRef, None)
  }

  def withReadFromRow(row: TermName, expr: Expression[_]): CodeGenResult = {
    withRowOperation(row, expr, State.RowOpType.ReadField, None)
  }

  def withReadFromRow(row: TermName, expr: Expression[_], valueDeclaration: ValueDeclaration): State = {
    withRowOperation(row, expr, State.RowOpType.ReadField, valueDeclaration, None)
  }

  def withWriteToRow(row: TermName, expr: Expression[_], valueDeclaration: ValueDeclaration): State = {
    this.withRowOperation(row, expr, State.RowOpType.WriteField, valueDeclaration, None)
  }

  def withWriteToRow(row: TermName, expr: Expression[_]): CodeGenResult = {
    this.withRowOperation(row, expr, State.RowOpType.WriteField, None)
  }
  private def withRowOperation(
      row: TermName,
      expr: Expression[_],
      opType: State.RowOpType.Value,
      tpe: Option[Tree]
  ): CodeGenResult = {
    rowOperations.find(op => op.row == row && op.expr == expr && op.opType == opType) match {
      case Some(op) =>
        CodeGenResult(Seq.empty, op.valueDeclaration, this)
      case None =>
        val idx = this.rowOperations.size
        val valName = TermName("field_" + idx)
        val validFlagName = TermName("field_valid_" + idx)
        val valueDecl = ValueDeclaration(valName, validFlagName)
        CodeGenResult(Seq.empty, valueDecl, withRowOperation(row, expr, opType, valueDecl, tpe))
    }
  }
  private def withRowOperation(
      row: TermName,
      expr: Expression[_],
      opType: State.RowOpType.Value,
      valueDeclaration: ValueDeclaration,
      tpe: Option[Tree]
  ): State = {
    val s = this
      .copy(
        rowOperations = this.rowOperations :+ RowOperation(row, expr, opType, valueDeclaration, tpe)
      )

    opType match {
      case State.RowOpType.WriteField | State.RowOpType.ReadField if expr.dataType.internalStorable.isRefType =>
        s.withRefExpression(expr)
      case State.RowOpType.WriteField | State.RowOpType.ReadField =>
        s.withExpression(expr)
      case State.RowOpType.WriteRef | State.RowOpType.ReadRef =>
        s.withRefExpression(expr)
      case _ => throw new IllegalStateException(s"Unknown $opType")
    }
  }

  def withNamedGlobal(name: TermName, tpe: Tree, tree: Tree): State = {
    if (!globalDecls.exists(_._1 == name))
      copy(globalDecls = globalDecls :+ (name -> Decl(name, tpe, tree)))
    else this
  }

  def withRef(ref: AnyRef, tpe: Tree): (TermName, State) = {
    refs.find(_._1 == ref) match {
      case Some((_, Decl(name, _, _))) => name -> this
      case None                        => withNewRef(ref, tpe)
    }
  }

  def withNewRef(ref: AnyRef, tpe: Tree): (TermName, State) = {
    val name = TermName(s"ref_${refs.size}")
    val refId = refs.size
    val tree = q"${JIT.REFS}($refId)"
    val ns = copy(refs = refs :+ (ref -> Decl(name, tpe, tree)))
    name -> ns
  }

  def withGlobal(key: Any, tpe: Tree, tree: Tree): (TermName, State) = {
    globalDecls.find(_._1 == key) match {
      case Some((_, Decl(name, _, _))) => name -> this
      case None                        =>
        val name = TermName(s"global_${globalDecls.size}")
        val ns =
          copy(globalDecls = globalDecls :+ (key -> Decl(name, tpe, tree)))
        name -> ns
    }
  }
}

object State {

  object RowOpType extends Enumeration {
    type RowOpType = Value
    val ReadField, WriteField, ReadRef, WriteRef = Value
  }

  case class RowOperation(
      row: TermName,
      expr: Expression[_],
      opType: RowOpType.Value,
      valueDeclaration: ValueDeclaration,
      tpe: Option[Tree]
  )
}
