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

import org.yupana.api.query.Expression
import org.yupana.api.types.InternalReaderWriter
import org.yupana.core.format.{ CompileReaderWriter, TypedTree }
import org.yupana.core.jit.State.RowOperation
import org.yupana.core.jit.{ State, ValueDeclaration }
import org.yupana.core.model.DatasetSchema

import scala.reflect.runtime.universe._

object BatchDatasetGen {

  implicit val rw: InternalReaderWriter[Tree, TypedTree, Tree, Tree] = CompileReaderWriter

  def mkSetValues(state: State, schema: DatasetSchema, rowId: Tree): Seq[Tree] = {
    mkSetValues(state, schema, None, rowId)
  }

  def mkSetValues(state: State, schema: DatasetSchema, dataset: TermName, rowId: Tree): Seq[Tree] = {
    mkSetValues(state, schema, Some(dataset), rowId)
  }
  def mkSetValues(state: State, schema: DatasetSchema, dataset: Option[TermName], rowId: Tree): Seq[Tree] = {
    val ts1 = state.rowOperations.distinct.collect {

      case RowOperation(ds, expr, opType, valueDeclaration, _) if opType == State.RowOpType.WriteRef =>
        mkSetRef(schema, expr)(valueDeclaration, rowId, q"${dataset.getOrElse(ds)}")

      case RowOperation(ds, expr, opType, valueDeclaration, _) if opType == State.RowOpType.WriteField =>
        mkSet(schema, rowId, expr)(valueDeclaration, q"${dataset.getOrElse(ds)}")
    }
    val datasets = (state.rowOperations
      .filter(o => o.opType == State.RowOpType.WriteField || o.opType == State.RowOpType.WriteRef)
      .map(_.row) ++ dataset.toSeq).distinct
    val ts2 = datasets.map { ds =>
      q"$ds.updateSize($rowId)"
    }
    ts1 ++ ts2
  }

  def mkSet(schema: DatasetSchema, rowId: Tree, expr: Expression[_])(
      valDecl: ValueDeclaration,
      dataset: Tree
  ): Tree = {
    if (expr.dataType.internalStorable.isRefType) {
      mkSetRef(schema, expr)(valDecl, rowId, dataset)
    } else {
      mkSetValueToRow(schema, rowId, expr)(valDecl, dataset)
    }
  }

  private def mkSetRef(schema: DatasetSchema, expr: Expression[_])(
      valueDeclaration: ValueDeclaration,
      rowId: Tree,
      dataset: Tree
  ): Tree = {
    val ordinal = schema.refFieldOrdinal(expr)
    if (expr.isNullable) {
      q"""
        if (${valueDeclaration.validityFlagName}) {
           ${mkSetNotNullRef(schema, expr)(valueDeclaration, rowId, dataset)}
        } else {
          $dataset.setRef($rowId, $ordinal, null)
        }
      """
    } else {
      mkSetNotNullRef(schema, expr)(valueDeclaration, rowId, dataset)
    }
  }

  private def mkSetNotNullRef(schema: DatasetSchema, expr: Expression[_])(
      valueDeclaration: ValueDeclaration,
      rowId: Tree,
      dataset: Tree
  ): Tree = {
    val ordinal = schema.refFieldOrdinal(expr)
    q"""
        $dataset.setRef($rowId, $ordinal, ${valueDeclaration.valueName})
    """
  }

  def mkSetValueToRow(
                       schema: DatasetSchema,
                       rowNum: Tree,
                       tag: Byte
  )(valName: Tree, dataset: Tree): Tree = {
    val index = schema.fieldIndex(tag)
    val expr = schema.getExpr(index)
    mkWriteValue(schema, rowNum, expr)(valName, dataset)
  }

  def mkSetValueToRow(
                       schema: DatasetSchema,
                       rowId: Tree,
                       expr: Expression[_]
  )(valDecl: ValueDeclaration, dataset: Tree): Tree = {
    val index = schema.fieldIndex(expr)
    if (index >= 0) {
      if (expr.isNullable) {
        q"""
           if (${valDecl.validityFlagName}) {
             ${mkWriteValue(schema, rowId, expr)(q"${valDecl.valueName}", dataset)}
           } else {
             $dataset.setNull($rowId, $index)
           }
           """
      } else {
        mkWriteValue(schema, rowId, expr)(q"${valDecl.valueName}", dataset)
      }
    } else {
      q"()"
    }
  }

  def mkWriteValue(
                    schema: DatasetSchema,
                    rowId: Tree,
                    expr: Expression[_]
  )(value: Tree, dataset: Tree): Tree = {

    val fieldIndex = schema.fieldIndex(expr)

    if (schema.isFixed(fieldIndex)) {
      val fieldOffset = schema.fieldOffset(fieldIndex)

      q"""
        val buf = $dataset.fixedLenFieldsBuffer
        val rowOffset = $rowId * ${schema.fixedLengthFieldsBytesSize}
        val offset = rowOffset + $fieldOffset
        ${expr.dataType.internalStorable.write[Tree, TypedTree, Tree, Tree](q"buf", q"offset", value)}
        $dataset.setValid($rowId, $fieldIndex)
      """
    } else {
      q"""
         val buf = $dataset.fieldBufferForWrite($rowId, $fieldIndex)
         val offset = $dataset.fieldBufferForWriteOffset($rowId, $fieldIndex)
         val size = ${expr.dataType.internalStorable.write[Tree, TypedTree, Tree, Tree](q"buf", q"offset", value)}
         $dataset.writeFieldBuffer($rowId, $fieldIndex, size)
         $dataset.setValid($rowId, $fieldIndex)
      """
    }
  }

  def mkGetValues(state: State, schema: DatasetSchema, rowId: Tree): Seq[Tree] = {
    mkGetValues(state, schema, None, rowId)
  }
  def mkGetValues(state: State, schema: DatasetSchema, dataset: TermName, rowId: Tree): Seq[Tree] = {
    mkGetValues(state, schema, Some(dataset), rowId)
  }

  def mkGetValues(state: State, schema: DatasetSchema, fromDataset: Option[TermName], rowId: Tree): Seq[Tree] = {
    state.rowOperations.distinct
      .filter(op => fromDataset.forall(_ == op.row))
      .collect {

        case RowOperation(dataset, expr, opType, valueDeclaration, tpe) if opType == State.RowOpType.ReadRef =>
          mkGetRef(schema, expr, dataset, rowId, valueDeclaration, tpe.getOrElse(CommonGen.mkType(expr)))

        case RowOperation(dataset, expr, opType, valueDeclaration, _) if opType == State.RowOpType.ReadField =>
          mkGet(schema, expr, dataset, rowId, valueDeclaration)
      }
      .flatten
  }

  def mkGet(
             schema: DatasetSchema,
             expr: Expression[_],
             dataset: TermName,
             rowId: Tree,
             valueDeclaration: ValueDeclaration
  ): Seq[Tree] = {
    if (expr.dataType.internalStorable.isRefType) {
      mkGetRef(schema, expr, dataset, rowId, valueDeclaration, CommonGen.mkType(expr))
    } else {
      mkGetValue(schema, expr, dataset, rowId, valueDeclaration)
    }
  }

  private def mkGetRef(
                        schema: DatasetSchema,
                        expr: Expression[_],
                        dataset: TermName,
                        rowId: Tree,
                        valueDeclaration: ValueDeclaration,
                        exprType: Tree
  ): Seq[Tree] = {
    val ordinal = schema.refFieldOrdinal(expr)
    val valueTree =
      q"val ${valueDeclaration.valueName}: $exprType = $dataset.getRef($rowId, $ordinal)"

    val validityTree =
      q"""
         val ${valueDeclaration.validityFlagName} = ${valueDeclaration.valueName} != null
       """
    Seq(valueTree, validityTree)
  }

  def mkGetValue(
                  schema: DatasetSchema,
                  expr: Expression[_],
                  dataset: TermName,
                  rowId: Tree,
                  valueDeclaration: ValueDeclaration
  ): Seq[Tree] = {
    if (expr.isNullable) {
      val index = schema.fieldIndex(expr)
      val validityFlagTree = mkReadValidityFlag(dataset, rowId, index, valueDeclaration)
      val valueTree =
        q"""
          val ${valueDeclaration.valueName} = if (${valueDeclaration.validityFlagName}) {
            ${mkReadValueFromRow(schema, expr)(dataset, rowId)}
          } else {
            ${CommonGen.initVal(expr)}
          }
          """
      Seq(validityFlagTree, valueTree)
    } else {
      val validityFlagTree = q"val ${valueDeclaration.validityFlagName} = true"
      val tree = q"val ${valueDeclaration.valueName} =  ${mkReadValueFromRow(schema, expr)(dataset, rowId)}"
      Seq(validityFlagTree, tree)
    }
  }

  private def mkReadValidityFlag(
      dataset: TermName,
      rowId: Tree,
      fieldIndex: Int,
      valueDeclaration: ValueDeclaration
  ): Tree = {
    q"val ${valueDeclaration.validityFlagName} = $dataset.isDefined($rowId, $fieldIndex)"
  }

  private def mkReadValueFromRow(
                                  schema: DatasetSchema,
                                  expr: Expression[_]
  )(dataset: TermName, rowId: Tree): Tree = {
    val fieldIndex = schema.fieldIndex(expr)

    val fieldOffset = schema.fieldOffset(fieldIndex)

    if (schema.isFixed(fieldIndex)) {
      val size = expr.dataType.internalStorable.fixedSize.getOrElse(0)

      q"""
         val buf = $dataset.fixedLenFieldsBuffer()
         val rowOffset = $rowId * ${schema.fixedLengthFieldsBytesSize}
         val offset = rowOffset + $fieldOffset
         ${expr.dataType.internalStorable.read[Tree, TypedTree, Tree, Tree](q"buf", q"offset", q"$size")}
      """
    } else {
      q"""
         val rowOffset = $rowId * ${schema.fixedLengthFieldsBytesSize}
         val fixedOffset = rowOffset + $fieldOffset
         val buf = $dataset.fieldBufferForReadVarLenField(fixedOffset)
         val offset = $dataset.fieldBufferForReadVarLenFieldOffset(fixedOffset)
         val size = $dataset.varLenFieldSize(fixedOffset)
         ${expr.dataType.internalStorable.read[Tree, TypedTree, Tree, Tree](q"buf", q"offset", q"size")}
     """
    }
  }
}
