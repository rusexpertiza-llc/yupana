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
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer
import org.yupana.core.jit.codegen.stages._
import org.yupana.core.jit.codegen.{ BatchDatasetGen, CommonGen }
import org.yupana.core.model.DatasetSchema

object JIT extends ExpressionCalculatorFactory with StrictLogging with Serializable {

  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  private val refs = TermName("refs")
  private val params = TermName("params")
  private val now = TermName("now")
  private val tokenizer = TermName("tokenizer")

  private val toolBox = currentMirror.mkToolBox()

  def makeCalculator(
      query: Query,
      condition: Option[Condition],
      tokenizer: Tokenizer
  ): (ExpressionCalculator, DatasetSchema) = {
    val (tree, refs, schema) = generateCalculator(query, condition)

    val res = compile(tree)(refs, query.params, query.startTime, tokenizer)

    (res, schema)
  }

  def compile(tree: Tree): (Array[Any], Array[Any], Time, Tokenizer) => ExpressionCalculator = {
    toolBox.eval(tree).asInstanceOf[(Array[Any], Array[Any], Time, Tokenizer) => ExpressionCalculator]
  }

  def generateCalculator(
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

    val schema = new DatasetSchema(postFilterState.valueExprIndex, postFilterState.refExprIndex, query.table)

    val rowNum = TermName("rowNum")
    val readRow = ReadFromStorageRowStage.mkReadRow(query, buf, schema, batch, rowNum)
    val tree =
      q"""
    import _root_.java.nio.ByteBuffer
    import _root_.org.yupana.serialization.MemoryBuffer
    import _root_.org.yupana.api.Time
    import _root_.org.yupana.api.types.DataType
    import _root_.org.yupana.api.utils.Tokenizer
    import _root_.org.yupana.api.schema.Table
    import _root_.org.yupana.core.model.{ BatchDataset, HashTableDataset }
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.threeten.extra.PeriodDuration
    import _root_.org.yupana.core.utils.Hash128Utils.timeHash

    ($refs: Array[Any], $params: Array[Any], $now: Time, $tokenizer: Tokenizer) =>
      new _root_.org.yupana.core.jit.ExpressionCalculator {

        ..$defs

        final class Key(val ds: BatchDataset, val rowNum: Int) {


           override def hashCode(): Int = {
              var h = 0
               ..${query.groupBy.zipWithIndex.map {
          case (expr, i) =>
            val valDecl = ValueDeclaration(s"keyField_$i")
            val read = BatchDatasetGen.mkGet(schema, expr, TermName("ds"), q"rowNum", valDecl)
            val mixfunc = if (i == 0) {
              q"valueHash"
            } else if (i < query.groupBy.size - 1) {
              q"scala.util.hashing.MurmurHash3.mix(h, valueHash)"
            } else {
              q"scala.util.hashing.MurmurHash3.finalizeHash(scala.util.hashing.MurmurHash3.mix(h, valueHash), ${query.groupBy.size})"
            }
            q"""
                         {
                            ..$read
                            val valueHash = if (${valDecl.validityFlagName}) ${valDecl.valueName}.hashCode() else 0
                            h = $mixfunc
                         }
                     """
        }}
                h
           }

           override def equals(that: scala.Any): Boolean = {
              var r = true
              ..${query.groupBy.zipWithIndex.map {
          case (expr, i) =>
            val valDecl1 = ValueDeclaration(s"keyField1_$i")
            val valDecl2 = ValueDeclaration(s"keyField2_$i")

            val read1 =
              BatchDatasetGen.mkGet(schema, expr, TermName("thatDs"), q"that.asInstanceOf[Key].rowNum", valDecl1)
            val read2 = BatchDatasetGen.mkGet(schema, expr, TermName("ds"), q"rowNum", valDecl2)
            q"""
                       val thatDs = that.asInstanceOf[Key].ds
                      ..$read1
                      ..$read2
                      r = r & (${valDecl1.validityFlagName} == ${valDecl2.validityFlagName}) & (${valDecl1.valueName} == ${valDecl2.valueName})
                   """
        }}
              r
           }
        }

        override def evaluateFilter($batch: BatchDataset): Unit = {
          var rowNum = 0
          while (rowNum < $batch.size) {
            ..${BatchDatasetGen.mkGetValues(filteredState, schema, q"rowNum")}
            val fl = {..$filter}
            if (!fl) {
               $batch.setDeleted(rowNum)
            }
            rowNum += 1
          }
        }

        override def evaluateExpressions($batch: BatchDataset): Unit = {
          ${if (projectionState.hasWriteOps) {
          q"""
               var rowNum = 0
               while (rowNum < $batch.size) {
                  if (!$batch.isDeleted(rowNum)) {
                    ..${BatchDatasetGen.mkGetValues(projectionState, schema, q"rowNum")}
                    ..$projection
                    ..${BatchDatasetGen.mkSetValues(projectionState, schema, q"rowNum")}
                  }
                  rowNum += 1
               }
            """
        } else { q"()" }}
        }

       override def createKey($batch: BatchDataset, rowNum: Int): AnyRef = {
          new Key($batch, rowNum)
       }

       override def evaluateFold($acc: HashTableDataset, $batch: BatchDataset): Unit = {

           var rowNum = 0
           while (rowNum < $batch.size) {
              if (!$batch.isDeleted(rowNum)) {

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
              }
              rowNum += 1
           }
        }

        override def evaluateCombine($acc: HashTableDataset, $batch: BatchDataset): Unit = {
           var rowNum = 0
           while (rowNum < $batch.size) {
              if (!$batch.isDeleted(rowNum)) {
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
                  ..${BatchDatasetGen.mkGetValues(reducedState, schema, batch, q"rowNum")}
                  val $accBatch = acc.batch(rowPtr)
                  val accRowNum = acc.rowNumber(rowPtr)
                  ..${BatchDatasetGen.mkGetValues(reducedState, schema, accBatch, q"accRowNum")}
                  ..$reduce
                  ..${BatchDatasetGen.mkSetValues(reducedState, schema, accBatch, q"accRowNum")}
                }
              }
              rowNum += 1
           }
        }

        override def evaluatePostCombine($batch: BatchDataset): Unit = {
           var rowNum = 0
           while (rowNum < $batch.size) {
              if (!$batch.isDeleted(rowNum)) {
               ..${BatchDatasetGen.mkGetValues(postCombineState, schema, q"rowNum")}
               ..$postCombine
               ..${BatchDatasetGen.mkSetValues(postCombineState, schema, q"rowNum")}
             }
             rowNum += 1
           }
        }

        def evaluatePostAggregateExprs($batch: BatchDataset): Unit = {
           var rowNum = 0
           while (rowNum < $batch.size) {
              if (!$batch.isDeleted(rowNum)) {
               ..${BatchDatasetGen.mkGetValues(postAggregateState, schema, q"rowNum")}
               ..$postAggregate
               ..${BatchDatasetGen.mkSetValues(postAggregateState, schema, q"rowNum")}
              }
              rowNum += 1
           }
        }

        override def evaluatePostFilter(batch: BatchDataset): Unit = {
           var rowNum = 0
           while (rowNum < $batch.size) {
              if (!$batch.isDeleted(rowNum)) {
               ..${BatchDatasetGen.mkGetValues(postFilterState, schema, q"rowNum")}
               val fl = {..$postFilter}
               if (!fl) {
                 $batch.setDeleted(rowNum)
               }
             }
             rowNum += 1
           }
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
