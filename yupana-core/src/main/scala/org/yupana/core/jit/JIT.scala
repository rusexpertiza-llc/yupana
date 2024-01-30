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

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.jit.codegen.stages._
import org.yupana.core.jit.codegen.InternalRowGen
import org.yupana.core.model.InternalRowBuilder

object JIT extends ExpressionCalculatorFactory with StrictLogging with Serializable {

  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  private val internalRowBuilder = TermName("internalRowBuilder")
  private val params = TermName("params")

  private val toolBox = currentMirror.mkToolBox()

  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int]) = {
    val (tree, known, params) = generateCalculator(query, condition)

    val res = compile(tree)(params)

    (res, known)
  }

  def compile(tree: Tree): Array[Any] => ExpressionCalculator = {
    toolBox.eval(tree).asInstanceOf[Array[Any] => ExpressionCalculator]
  }
  def generateCalculator(query: Query, condition: Option[Condition]): (Tree, Map[Expression[_], Int], Array[Any]) = {
    val internalRow = TermName("internalRow")
    val initialState =
      State(
        Map.empty,
        query.fields.map(_.expr).toSet ++ query.groupBy ++ query.postFilter + TimeExpr,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Map.empty
      )

    val (filter, filteredState) = FilterStageGen.mkFilter(initialState, internalRow, condition)

    val (projection, projectionState) = ProjectionStageGen.mkProjection(query, internalRow, filteredState.fresh())

    val acc = TermName("acc")

    val (zero, mappedState) = FoldStageGen.mkZero(projectionState.fresh(), query, internalRow)

    val (fold, foldedState) = FoldStageGen.mkFold(mappedState.fresh(), query, acc, internalRow)

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val (reduce, reducedState) = CombineStageGen.mkCombine(foldedState.fresh(), query, rowA, rowB)

    val (postMap, postMappedState) =
      PostCombineStageGen.mkPostCombine(reducedState.fresh(), query, internalRow)

    val (postAggregate, postAggregateState) =
      PostAgrregateStageGen.mkPostAggregate(query, internalRow, postMappedState.fresh())

    val (postFilter, finalState) = PostFilterStageGen.mkPostFilter(postAggregateState.fresh(), internalRow, query)

    val defs = finalState.globalDecls.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}" } ++
      finalState.refs.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}.asInstanceOf[${d.tpe}]" }

    val buf = TermName("buf")

    val builder = new InternalRowBuilder(finalState.index, query.table)
    val readRow = ReadFromStorageRowStage.mkReadRow(query, buf, internalRowBuilder, builder)

    val tree =
      q"""
    import _root_.java.nio.ByteBuffer
    import _root_.org.yupana.readerwriter.MemoryBuffer
    import _root_.org.yupana.api.Time
    import _root_.org.yupana.api.types.DataType
    import _root_.org.yupana.api.utils.Tokenizer
    import _root_.org.yupana.api.schema.Table
    import _root_.org.yupana.core.model.{InternalRow, InternalRowBuilder}
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.yupana.core.utils.Hash128Utils.timeHash

    ($params: Array[Any]) =>
      new _root_.org.yupana.core.jit.ExpressionCalculator {
        ..$defs

        override def evaluateFilter($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): Boolean = {
          ..${InternalRowGen.mkGetValuesFromRow(filteredState, builder, internalRowBuilder)}
          val res = {..$filter}
          res
        }

        override def evaluateExpressions($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): InternalRow = {
          ${if (projectionState.hasWriteOps) {
          q"""
                ..${InternalRowGen.mkGetValuesFromRow(projectionState, builder, internalRowBuilder)}
                ..$projection
                ..${InternalRowGen.mkSetValuesToRow(projectionState, builder, internalRowBuilder)}
                """
        } else {
          q"$internalRow"
        }}
        }

        override def evaluateZero($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): InternalRow = {
          ..${InternalRowGen.mkGetValuesFromRow(mappedState, builder, internalRowBuilder)}
          ..$zero
          ..${InternalRowGen.mkSetValuesToRow(mappedState, builder, internalRowBuilder)}
        }

        override def evaluateSequence($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $acc: InternalRow, $internalRow: InternalRow): InternalRow = {
          ..${InternalRowGen.mkGetValuesFromRow(foldedState, builder, internalRowBuilder)}
          ..$fold
          ..${InternalRowGen.mkSetValuesToRow(foldedState, builder, internalRowBuilder)}
        }

        override def evaluateCombine($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $rowA: InternalRow, $rowB: InternalRow): InternalRow = {
          ..${InternalRowGen.mkGetValuesFromRow(reducedState, builder, internalRowBuilder)}
          ..$reduce
          ..${InternalRowGen.mkSetValuesToRow(reducedState, builder, internalRowBuilder)}
        }

        override def evaluatePostMap($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): InternalRow = {
          ${if (postMappedState.hasWriteOps) {
          q"""
              ..${InternalRowGen.mkGetValuesFromRow(postMappedState, builder, internalRowBuilder)}
              ..$postMap
              ..${InternalRowGen.mkSetValuesToRow(postMappedState, builder, internalRowBuilder)}
              """
        } else {
          q"$internalRow"
        }}
        }

        override def evaluatePostAggregateExprs($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): InternalRow = {
          ${if (postAggregateState.hasWriteOps) {
          q"""
          ..${InternalRowGen.mkGetValuesFromRow(postAggregateState, builder, internalRowBuilder)}
          ..$postAggregate
          ..${InternalRowGen.mkSetValuesToRow(postAggregateState, builder, internalRowBuilder)}
          """
        } else {
          q"$internalRow"
        }}
        }

        override def evaluatePostFilter($internalRowBuilder: InternalRowBuilder, ${ExpressionCodeGenFactory.tokenizer}: Tokenizer, $internalRow: InternalRow): Boolean = {
           ..${InternalRowGen.mkGetValuesFromRow(finalState, builder, internalRowBuilder)}
           ..$postFilter
        }

        override def evaluateReadRow($buf: MemoryBuffer, $internalRowBuilder: InternalRowBuilder): InternalRow = {
          $readRow
         }
      }
  """

    val finalRequirements = finalState.required -- finalState.index.keySet
    val index = finalRequirements.foldLeft(finalState.index)((i, e) => i + (e -> i.size))
    val paramsArray = finalState.refs.map(_._1).toArray[Any]

    logger.whenTraceEnabled {
      val sortedIndex = index.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      sortedIndex.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Params size: ${paramsArray.length}")
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tree, index, paramsArray)
  }

  private def prettyTree(tree: Tree): String = {
    show(tree)
      .replaceAll("_root_\\.([a-z_]+\\.)+", "")
      .replaceAll("\\.\\$bang\\$eq", " != ")
      .replaceAll("\\.\\$eq\\$eq", " == ")
      .replaceAll("\\.\\$amp\\$amp", " && ")
      .replaceAll("\\.\\$bar\\$bar", " || ")
      .replaceAll("\\.\\$bar", " | ")
      .replaceAll("\\.\\$percent", " % ")
      .replaceAll("\\.\\$plus\\$plus", " ++ ")
      .replaceAll("\\$plus\\$plus", "++")
      .replaceAll("\\.\\$plus\\$eq", " += ")
      .replaceAll("\\.\\$plus", " + ")
      .replaceAll("\\$plus", "+")
      .replaceAll("\\.\\$minus", " - ")
      .replaceAll("\\.\\$div", " / ")
      .replaceAll("\\.\\$greater\\$eq", " >= ")
      .replaceAll("\\.\\$greater", " > ")
      .replaceAll("\\.\\$less\\$eq", " <= ")
      .replaceAll("\\.\\$less\\$less", " << ")
      .replaceAll("\\.\\$less", " < ")
  }

}
