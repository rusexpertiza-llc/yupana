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
import org.yupana.api.utils.Tokenizer
import org.yupana.core.jit.codegen.stages._
import org.yupana.core.jit.codegen.{ BatchDatasetGen, CommonGen, KeyGen }
import org.yupana.core.model.DatasetSchema

object JIT extends ExpressionCalculatorFactory with StrictLogging with Serializable {

  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  val REFS = TermName("refs")
  val PARAMS = TermName("params")
  val NOW = TermName("now")
  private val tokenizer = TermName("tokenizer")

  private val toolBox = currentMirror.mkToolBox()

  def makeCalculator(
      query: Query,
      condition: Option[Condition],
      tokenizer: Tokenizer
  ): (ExpressionCalculator, DatasetSchema) = {
    val (tree, refs, schema) = generateCalculator(query, condition)

    val res = compile(tree)(refs, tokenizer)

    (res, schema)
  }

  private[jit] def compile(tree: Tree): (Array[Any], Tokenizer) => ExpressionCalculator = {
    toolBox.eval(tree).asInstanceOf[(Array[Any], Tokenizer) => ExpressionCalculator]
  }

  private[jit] def generateCalculator(
      query: Query,
      condition: Option[Condition]
  ): (Tree, Array[Any], DatasetSchema) = {
    val batch = TermName("batch")
    val initialState =
      State(
        Map.empty,
        Map.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Map.empty
      ).withExpression(TimeExpr)

    val (filter, filteredState) = FilterStageGen.mkFilter(initialState, batch, condition)

    val (projection, projectionState) = ProjectionStageGen.mkProjection(query, batch, filteredState.fresh())

    val acc = TermName("acc")
    val accBatch = TermName("accBatch")

    val (zero, zeroState) = FoldStageGen.mkZero(projectionState.fresh(), query, accBatch, batch)

    val (fold, foldedState) = FoldStageGen.mkFold(zeroState.fresh(), query, accBatch, batch)

    val (reduce, reducedState) = CombineStageGen.mkCombine(foldedState.fresh(), query, accBatch, batch)

    val (postCombine, postCombineState) =
      PostCombineStageGen.mkPostCombine(reducedState.fresh(), query, batch)

    val (postAggregate, postAggregateState) =
      PostAgrregateStageGen.mkPostAggregate(query, batch, postCombineState.fresh())

    val (postFilter, postFilterState) = PostFilterStageGen.mkPostFilter(postAggregateState.fresh(), batch, query)

    val defs = postFilterState.globalDecls.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}" } ++
      postFilterState.refs.map { case (_, d) => q"private val ${d.name}: ${d.tpe} = ${d.value}.asInstanceOf[${d.tpe}]" }

    val buf = TermName("buf")

    val refsArray = postFilterState.refs.map(_._1).toArray[Any]

    val nameMapping = query.fields.map { f =>
      val fieldIndex = postFilterState.valueExprIndex.getOrElse(f.expr, postFilterState.refExprIndex(f.expr))
      f.name -> fieldIndex
    }.toMap

    val schema = new DatasetSchema(
      postFilterState.valueExprIndex,
      postFilterState.refExprIndex,
      nameMapping,
      query.table
    )

    val rowNum = TermName("rowNum")
    val readRow = ReadFromStorageRowStage.mkReadRow(query, buf, schema, batch, rowNum)
    val tree =
      q"""
    import _root_.org.yupana.serialization.MemoryBuffer
    import _root_.org.yupana.api.{ Time, Blob, Currency }
    import _root_.org.yupana.api.types.{ DataType, Num }
    import _root_.org.yupana.api.utils.Tokenizer
    import _root_.org.yupana.api.schema.Table
    import _root_.org.yupana.core.model.{ BatchDataset, HashTableDataset }
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.yupana.core.utils.Hash128Utils.timeHash

    ($REFS: Array[Any], $tokenizer: Tokenizer) =>
      new _root_.org.yupana.core.jit.ExpressionCalculator {

        ..$defs

        final class Key(val ds: BatchDataset, val rowNum: Int) {
           override def hashCode(): Int = {
              ${KeyGen.mkHashCode(query, schema)}
           }

           override def equals(that: scala.Any): Boolean = {
              ${KeyGen.mkEquals(query, schema)}
           }
        }

        override def evaluateFilter($batch: BatchDataset, $NOW: Time, $PARAMS: IndexedSeq[Any]): Unit = {
          ${filterBody(filter, filteredState, schema, batch)}
        }

        override def evaluateExpressions($batch: BatchDataset, $NOW: Time, $PARAMS: IndexedSeq[Any]): Unit = {
          ${evaluateBody(projection, projectionState, schema, batch)}
        }

       override def createKey($batch: BatchDataset, rowNum: Int): AnyRef = {
          new Key($batch, rowNum)
       }

       override def evaluateFold($acc: HashTableDataset, $batch: BatchDataset, $NOW: Time, $PARAMS: IndexedSeq[Any]): Unit = {
         ${evaluateFoldBody(zero, zeroState, fold, foldedState, query, schema, batch, acc, accBatch)}
        }

        override def evaluateCombine($acc: HashTableDataset, $batch: BatchDataset): Unit = {
           ${combineBody(reduce, reducedState, query, schema, batch, acc, accBatch)}
        }

        override def evaluatePostCombine($batch: BatchDataset): Unit = {
          ${postCombineBody(postCombine, postCombineState, schema, batch)}
        }

        def evaluatePostAggregateExprs($batch: BatchDataset): Unit = {
           ${evaluatePostAggregateBody(postAggregate, postAggregateState, schema, batch)}
        }

        override def evaluatePostFilter(batch: BatchDataset, $NOW: Time, $PARAMS: IndexedSeq[Any]): Unit = {
          ${filterBody(postFilter, postFilterState, schema, batch)}
        }

        override def evaluateReadRow($buf: MemoryBuffer, $batch: BatchDataset, $rowNum: Int): Unit = {
          $readRow
        }
      }
  """

    logger.whenTraceEnabled {
      val sortedIndex = schema.exprIndex.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      sortedIndex.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Refs size: ${refsArray.length}")
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tree, refsArray, schema)
  }

  private def filterBody(filter: Seq[Tree], state: State, schema: DatasetSchema, batch: TermName): Tree = {
    if (filter.nonEmpty) {
      q"""
        var rowNum = 0
        while (rowNum < $batch.size) {
          ..${BatchDatasetGen.mkGetValues(state, schema, q"rowNum")}
          val fl = {..$filter}
          if (!fl) {
            $batch.setDeleted(rowNum)
          }
          rowNum += 1
        }
      """
    } else q"()"
  }

  private def evaluateBody(projection: Seq[Tree], state: State, schema: DatasetSchema, batch: TermName): Tree = {
    if (state.hasWriteOps) {
      foreachUndeleted(
        batch,
        q"""
          ..${BatchDatasetGen.mkGetValues(state, schema, q"rowNum")}
          ..$projection
          ..${BatchDatasetGen.mkSetValues(state, schema, q"rowNum")}
        """
      )
    } else q"()"
  }

  private def evaluateFoldBody(
      zero: Seq[Tree],
      zeroState: State,
      fold: Seq[Tree],
      foldedState: State,
      query: Query,
      schema: DatasetSchema,
      batch: TermName,
      acc: TermName,
      accBatch: TermName
  ): Tree = {
    if (zero.nonEmpty || fold.nonEmpty || query.groupBy.nonEmpty) {
      foreachUndeleted(
        batch,
        q"""
        val key = new Key($batch, rowNum)
        val rowPtr = $acc.rowPointer(key)
        if (rowPtr == HashTableDataset.KEY_NOT_FOUND) {
          ..${CommonGen.mkReadGroupByFields(query, schema, batch, q"rowNum")}
          ..${BatchDatasetGen.mkGetValues(zeroState, schema, batch, q"rowNum")}
          ..$zero
          val ptr = acc.newRow()
          val $accBatch = acc.batch(ptr)
          val accRowNum = acc.rowNumber(ptr)
          ..${CommonGen.mkWriteGroupByFields(query, schema, accBatch, q"accRowNum")}
          ..${BatchDatasetGen.mkSetValues(zeroState, schema, accBatch, q"accRowNum")}
          acc.updateKey(new Key($accBatch, accRowNum), ptr)
        } else {
          ..${BatchDatasetGen.mkGetValues(foldedState, schema, batch, q"rowNum")}
          val $accBatch = acc.batch(rowPtr)
          val accRowNum = acc.rowNumber(rowPtr)
          ..${BatchDatasetGen.mkGetValues(foldedState, schema, accBatch, q"accRowNum")}
          ..$fold
          ..${BatchDatasetGen.mkSetValues(foldedState, schema, accBatch, q"accRowNum")}
        }
        """
      )
    } else q"()"
  }

  private def combineBody(
      reduce: Seq[Tree],
      state: State,
      query: Query,
      schema: DatasetSchema,
      batch: TermName,
      acc: TermName,
      accBatch: TermName
  ): Tree = {
    if (reduce.nonEmpty) {
      foreachUndeleted(
        batch,
        q"""
        val key = new Key($batch, rowNum)
        val rowPtr = $acc.rowPointer(key)
        if (rowPtr == HashTableDataset.KEY_NOT_FOUND) {
          ..${CommonGen.mkReadGroupByFields(query, schema, batch, q"rowNum")}
          val ptr = acc.newRow()
          val $accBatch = acc.batch(ptr)
          val accRowNum = acc.rowNumber(ptr)
           ${CommonGen.copyAggregateFields(batch, q"rowNum", accBatch, q"accRowNum")}
           ..${CommonGen.mkWriteGroupByFields(query, schema, accBatch, q"accRowNum")}
          acc.updateKey(new Key($accBatch, accRowNum), ptr)
        } else {
          ..${BatchDatasetGen.mkGetValues(state, schema, batch, q"rowNum")}
          val $accBatch = acc.batch(rowPtr)
          val accRowNum = acc.rowNumber(rowPtr)
          ..${BatchDatasetGen.mkGetValues(state, schema, accBatch, q"accRowNum")}
          ..$reduce
          ..${BatchDatasetGen.mkSetValues(state, schema, accBatch, q"accRowNum")}
        }
     """
      )
    } else q"()"
  }

  private def postCombineBody(postCombine: Seq[Tree], state: State, schema: DatasetSchema, batch: TermName): Tree = {
    if (postCombine.nonEmpty) {
      foreachUndeleted(
        batch,
        q"""
        ..${BatchDatasetGen.mkGetValues(state, schema, q"rowNum")}
        ..$postCombine
        ..${BatchDatasetGen.mkSetValues(state, schema, q"rowNum")}
      """
      )
    } else q"()"
  }

  private def evaluatePostAggregateBody(
      postAggregate: Seq[Tree],
      state: State,
      schema: DatasetSchema,
      batch: TermName
  ): Tree = {
    if (postAggregate.nonEmpty) {
      foreachUndeleted(
        batch,
        q"""
        ..${BatchDatasetGen.mkGetValues(state, schema, q"rowNum")}
        ..$postAggregate
        ..${BatchDatasetGen.mkSetValues(state, schema, q"rowNum")}
      """
      )
    } else q"()"
  }

  private def foreachUndeleted(batch: TermName, t: Tree): Tree = {
    q"""
       var rowNum = 0
       while (rowNum < $batch.size) {
         if (!$batch.isDeleted(rowNum)) {
           $t
         }
         rowNum += 1
       }
     """
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
      .replaceAll("\\.\\$times", " * ")
      .replaceAll("\\.\\$greater\\$eq", " >= ")
      .replaceAll("\\.\\$greater", " > ")
      .replaceAll("\\.\\$less\\$eq", " <= ")
      .replaceAll("\\.\\$less\\$less", " << ")
      .replaceAll("\\.\\$less", " < ")
  }

}
