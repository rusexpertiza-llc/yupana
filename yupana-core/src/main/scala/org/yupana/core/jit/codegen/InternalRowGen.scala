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
import org.yupana.api.types.ReaderWriter
import org.yupana.core.format.{ CompileReaderWriter, TypedTree }
import org.yupana.core.jit.{ State, ValueDeclaration }
import org.yupana.core.jit.State.RowOperation
import org.yupana.core.model.InternalRowBuilder

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object InternalRowGen {

  implicit val rw: ReaderWriter[Tree, TypedTree, TypedTree] = CompileReaderWriter

  def mkSetValuesToRow(state: State, builder: InternalRowBuilder, builderName: TermName): Seq[universe.Tree] = {
    val ts = state.rowOperations.collect {
      case RowOperation(row, expr, opType, valueDeclaration) if opType == State.RowOpType.WriteField =>
        mkSetValueToRow(builder, expr)(valueDeclaration, q"$builderName.fieldsBuffer", builderName)

      case RowOperation(row, expr, opType, valueDeclaration) if opType == State.RowOpType.WriteRef =>
        mkSetRef(builder, expr)(valueDeclaration, builderName)
    }

    ts :+ q"$builderName.buildAndReset"
  }
  private def mkSetRef(builder: InternalRowBuilder, expr: Expression[_])(
      valueDeclaration: ValueDeclaration,
      builderName: TermName
  ): Tree = {
    val index = builder.exprIndex(expr)
    if (expr.isNullable) {
      q"""
        if (${valueDeclaration.validityFlagName}) {
          $builderName.setRef($index, ${valueDeclaration.valueName})
          $builderName.setValid($index)
        } else {
          $builderName.setNull($index)
        }
      """
    } else {
      q"""
        $builderName.setRef($index, ${valueDeclaration.valueName})
        $builderName.setValid($index)
      """
    }
  }

  def mkSetValueToRow(
      builder: InternalRowBuilder,
      tag: Byte
  )(valName: Tree, rowBuffer: Tree, builderName: TermName): Tree = {
    val index = builder.tagIndex(tag)
    mkWriteValueToRow(builder, index)(valName, rowBuffer, builderName)
  }

  def mkSetValueToRow(
      builder: InternalRowBuilder,
      expr: Expression[_]
  )(valDecl: ValueDeclaration, rowBuffer: Tree, builderName: TermName): Tree = {
    val index = builder.exprIndex(expr)
    if (index >= 0) {
      if (expr.isNullable) {
        q"""
           if (${valDecl.validityFlagName}) {
             ${mkWriteValueToRow(builder, index)(q"${valDecl.valueName}", rowBuffer, builderName)}
           } else {
             InternalRowBuilder.setNull($rowBuffer, $index)
           }
           """
      } else {
        mkWriteValueToRow(builder, index)(q"${valDecl.valueName}", rowBuffer, builderName)
      }
    } else {
      q"()"
    }
  }

  def mkWriteValueToRow(
      builder: InternalRowBuilder,
      index: Int
  )(value: Tree, rowBuffer: Tree, builderName: TermName): Tree = {
    val offset = builder.fieldOffset(index)
    val expr = builder.getExpr(index)
    if (builder.isFixed(index)) {
      q"""
      ${expr.dataType.internalStorable.write[Tree, TypedTree, TypedTree](rowBuffer, offset, value)}
      InternalRowBuilder.setValid($rowBuffer, $index)
     """
    } else {
      q"""
          val bb = $builderName.tmpBuffer
          bb.rewind()
          ${expr.dataType.internalStorable.write[Tree, TypedTree, TypedTree](q"bb", value)}
          val len = bb.position()
          bb.rewind()
          $builderName.setVariableLengthValueFromBuf($rowBuffer, $index, bb, len)
          InternalRowBuilder.setValid($rowBuffer, $index)
         """
    }

  }

  def mkGetValuesFromRow(state: State, builder: InternalRowBuilder, builderName: TermName): Seq[Tree] = {
    state.rowOperations.collect {
      case RowOperation(row, expr, opType, valueDeclaration) if opType == State.RowOpType.ReadField =>
        mkGetValueFromRow(builder, expr)(row, builderName: TermName, valueDeclaration)

      case RowOperation(row, expr, opType, valueDeclaration) if opType == State.RowOpType.ReadRef =>
        mkGetRef(builder, expr, row, valueDeclaration)

    }.flatten
  }

  private def mkGetRef(
      builder: InternalRowBuilder,
      expr: Expression[_],
      row: TermName,
      valueDeclaration: ValueDeclaration
  ): Seq[universe.Tree] = {
    val index = builder.exprIndex(expr)
    val validityTree = mkReadValidityFlag(row, index, valueDeclaration)
    val valueTree =
      q"""
          val ${valueDeclaration.valueName} = if (${valueDeclaration.validityFlagName}) {
             $row.getRef($index)
          } else {
             null
          }
        """
    Seq(validityTree, valueTree)
  }

  private def mkGetValueFromRow(
      builder: InternalRowBuilder,
      expr: Expression[_]
  )(row: TermName, builderName: TermName, valueDeclaration: ValueDeclaration): Seq[Tree] = {
    if (expr.isNullable) {
      val index = builder.exprIndex(expr)
      val validityFlagTree = mkReadValidityFlag(row, index, valueDeclaration)
      val valueTree =
        q"""
          val ${valueDeclaration.valueName} = if (${valueDeclaration.validityFlagName}) {
            ${mkReadValueFromRow(builder, expr)(row, builderName)}
          } else {
            ${CommonGen.initVal(expr)}
          }
          """
      Seq(validityFlagTree, valueTree)
    } else {
      val validityFlagTree = q"val ${valueDeclaration.validityFlagName} = true"
      val tree = q"val ${valueDeclaration.valueName} =  ${mkReadValueFromRow(builder, expr)(row, builderName)}"
      Seq(validityFlagTree, tree)
    }
  }

  private def mkReadValidityFlag(row: TermName, index: Int, valueDeclaration: ValueDeclaration): Tree = {
    q"val ${valueDeclaration.validityFlagName} = InternalRowBuilder.isValid($row.bytes, $index)"
  }

  private def mkReadValueFromRow(
      builder: InternalRowBuilder,
      expr: Expression[_]
  )(row: TermName, builderName: TermName): Tree = {
    val index = builder.exprIndex(expr)
    val offset = builder.fieldOffset(index)
    if (builder.isFixed(index)) {
      q"""
       ${expr.dataType.internalStorable.read[Tree, TypedTree, TypedTree](q"$row.bytes", offset)}
       """
    } else {
      q"""
          val len = $row.bytes.getInt($offset)
          if (len <= 12) {
             ${expr.dataType.internalStorable.read[Tree, TypedTree, TypedTree](q"$row.bytes", offset + 4)}
          } else {
             val vOffset = $row.bytes.getInt($offset + 4)
             $builderName.tmpBuffer.put(0, $row.bytes, $offset + 8, 8)
             $builderName.tmpBuffer.put(8, $row.bytes, vOffset, len - 8)
             ${expr.dataType.internalStorable.read[Tree, TypedTree, TypedTree](q"$builderName.tmpBuffer", 0)}
          }
       """
    }
  }
}
