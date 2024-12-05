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

package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ ExternalLink, Schema, Table }
import org.yupana.api.utils.CloseableIterator
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.jit.{ CachingExpressionCalculatorFactory, ExpressionCalculatorFactory }
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.metric._

import scala.collection.mutable

class TSDB(
    override val schema: Schema,
    override val dao: TSDao[Iterator, Long],
    val changelogDao: ChangelogDao,
    override val prepareQuery: Query => Query,
    config: TsdbConfig,
    metricCollectorCreator: (Query, String) => MetricQueryCollector
) extends TsdbBase
    with StrictLogging {

  override type Collection[X] = Iterator[X]
  override type Result = TsdbServerResult

  override lazy val extractBatchSize: Int = config.extractBatchSize
  override lazy val putBatchSize: Int = config.putBatchSize

  override val calculatorFactory: ExpressionCalculatorFactory = CachingExpressionCalculatorFactory

  private var externalLinks = Map.empty[ExternalLink, ExternalLinkService[_ <: ExternalLink]]

  def registerExternalLink(
      externalLink: ExternalLink,
      externalLinkService: ExternalLinkService[_ <: ExternalLink]
  ): Unit = {
    externalLinks += (externalLink -> externalLinkService)
  }

  override def put(dataPoints: Collection[DataPoint], user: YupanaUser = YupanaUser.ANONYMOUS): Unit = {
    if (config.putEnabled) {
      super.put(dataPoints, user)
    } else throw new IllegalAccessException("Put is disabled")
  }

  override def putDataset(
      tables: Seq[Table],
      dataset: Collection[BatchDataset],
      user: YupanaUser
  ): Unit = {
    if (config.putEnabled) {
      super.putDataset(tables, dataset, user)
    } else throw new IllegalAccessException("Put is disabled")
  }

  override def createMetricCollector(query: Query, user: YupanaUser): MetricQueryCollector =
    if (config.collectMetrics) metricCollectorCreator(query, user.name)
    else NoMetricCollector

  override def finalizeQuery(
      queryContext: QueryContext,
      data: Iterator[BatchDataset],
      metricCollector: MetricQueryCollector
  ): TsdbServerResult = {
    val it = CloseableIterator(data, metricCollector.finish())
    new TsdbServerResult(queryContext, it)
  }

  override def applyWindowFunctions(
      queryContext: QueryContext,
      batches: Iterator[BatchDataset]
  ): Iterator[BatchDataset] = {
    val batchesArray = batches.toArray

    val groups = mutable.AnyRefMap.empty[AnyRef, mutable.ArrayBuffer[Long]]

    val keyDataRowIdSeq = mutable.ArrayBuffer.empty[(AnyRef, Long)]

    batchesArray.zipWithIndex.foreach {
      case (batch, batchIdx) =>
        var batchRowNum = 0
        while (batchRowNum < batch.size) {
          if (!batch.isDeleted(batchRowNum)) {
            val key = queryContext.calculator.createKey(batch, batchRowNum)
            val rowId = (batchIdx.toLong << 32) + batchRowNum.toLong
            val rowIds = groups.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
            rowIds.append(rowId)
            keyDataRowIdSeq.append(key -> rowId)
          }
          batchRowNum += 1
        }
    }

    val sortedGroups = groups.mapValuesNow { rowIds =>
      rowIds.sortInPlaceBy { rowId =>
        val batchIdx = (rowId >> 32).toInt
        val rowNum = (rowId & 0xFFFFFFFFL).toInt
        batchesArray(batchIdx).get[Time](rowNum, TimeExpr)
      }

      val rowIdToPosMap = rowIds.zipWithIndex.toMap
      rowIdToPosMap
    }

    val winExprGroupsWithValues = queryContext.query.fields.map(_.expr).collect {
      case winFuncExpr: WindowFunctionExpr[_, _] =>
        val values = sortedGroups.mapValuesNow { rowIdToPosMap =>
          val funcValues = winFuncExpr.expr.dataType.classTag.newArray(rowIdToPosMap.size)
          rowIdToPosMap.foreachEntry { (rowId, _) =>
            val batchIdx = (rowId >> 32).toInt
            val rowNum = (rowId & 0xFFFFFFFFL).toInt
            funcValues(rowIdToPosMap(rowId)) = batchesArray(batchIdx).get(rowNum, winFuncExpr.expr)
          }
          (funcValues, rowIdToPosMap)
        }
        winFuncExpr -> values
    }

    keyDataRowIdSeq.foreach {
      case (key, rowId) =>
        winExprGroupsWithValues.foreach {
          case (winFuncExpr, groupValues) =>
            val (values, rowIdToPosMap) = groupValues(key)
            val pos = rowIdToPosMap(rowId)
            val value = queryContext.calculator.evaluateWindow(winFuncExpr, values, pos)
            if (value != null) {
              val batchIdx = (rowId >> 32).toInt
              val rowNum = (rowId & 0xFFFFFFFFL).toInt
              batchesArray(batchIdx).set(rowNum, winFuncExpr, value)
            }
        }
    }
    batchesArray.iterator
  }

  override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    externalLinks.getOrElse(
      catalog,
      throw new Exception(s"Can't find catalog ${catalog.linkName}: ${catalog.fields}")
    )
  }

  override def externalLinkServices: Iterable[ExternalLinkService[_]] = externalLinks.values
}
