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

import org.yupana.api.query._
import org.yupana.api.schema.{ RawDimension, Table }
import org.yupana.api.types.ReaderWriter
import org.yupana.core.format.{ CompileReaderWriter, TypedTree }
import org.yupana.core.jit.ValueDeclaration
import org.yupana.core.jit.codegen.InternalRowGen
import org.yupana.core.model.InternalRowBuilder

import scala.reflect.runtime.universe._
object ReadFromStorageRowStage {

  def mkReadRow(
      query: Query,
      buf: TermName,
      internalRowBuilderName: TermName,
      builder: InternalRowBuilder
  ): Tree = {
    implicit val rw: ReaderWriter[Tree, TypedTree, TypedTree] = CompileReaderWriter

    val rowBuffer = TermName("rowBuffer")
    val b = q"$buf"
    val (rOffset, dims) = query.table.toSeq.flatMap(_.dimensionSeq).zipWithIndex.foldLeft((8, Seq.empty[Tree])) {
      case ((offset, cs), (dim, i)) =>
        val read = dim.rStorable.read[Tree, TypedTree, TypedTree](b, offset)(rw)
        val tag = (Table.DIM_TAG_OFFSET + i).toByte
        val c = dim match {
          case d: RawDimension[_] if builder.tagIndex(tag) >= 0 =>
            q"""
              val v = { $read }
              ${InternalRowGen
                .mkSetValueToRow(builder, tag)(
                  q"v",
                  q"$rowBuffer",
                  internalRowBuilderName
                )}
              """

          case _ =>
            q"""
             ()
            """
        }
        (offset + dim.rStorable.size, cs :+ c)
    }

    val readValCases = query.table.toSeq
      .flatMap(_.metrics)
      .map { metric =>

        val read = metric.dataType.storable.read[Tree, TypedTree, TypedTree](b)(rw)
        cq"""
       ${metric.tag} =>
          ${if (builder.tagIndex(metric.tag) >= 0) {
            q"""
                val v =  $read
                ${InternalRowGen.mkSetValueToRow(builder, metric.tag)(q"v", q"$rowBuffer", internalRowBuilderName)}
             """
          } else { q"$read" }}
       """
      }

    val tableName = query.table.map(_.name).getOrElse("")

    val metrics =
      q"""
      $b.position($rOffset + 8)
      while ($b.hasRemaining) {
        val tag = $b.get()
        tag match {
          case ..$readValCases
          case _ => throw new IllegalStateException("Unknown tag: " + tag + " in table " + $tableName)
        }
      }
  """

    val vmask = (0 until builder.validityMapAreaSize / 8).map { i =>
      val name = TermName("vmask_" + i)
      q"val $name = 0L"
    }

    val r =
      q"""
      ..$vmask
      val $rowBuffer = $internalRowBuilderName.fieldsBuffer
      val baseTime = $b.getLong()
      ..$dims
      val restTime = $b.getLong()
      $metrics
      val time = Time(baseTime + restTime)
      ${if (builder.timeIndex >= 0) {
          InternalRowGen.mkSetValueToRow(builder, TimeExpr)(
            ValueDeclaration("time"),
            q"$rowBuffer",
            internalRowBuilderName
          )
        } else { q"()" }}
      $internalRowBuilderName.buildAndReset
    """
    r
  }
}
