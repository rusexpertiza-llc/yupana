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
import org.yupana.api.schema.{ ExternalLink, Table }
import org.yupana.core.dao.{ DictionaryProvider, TSDao, TsdbQueryMetricsDao }
import org.yupana.core.model.{ InternalRow, KeyData }
import org.yupana.core.utils.metric._

import scala.collection.AbstractIterator

// NOTE: dao is TSDaoHBase because TSDB has put and rollup related method.  Possible it better to not have them here
class TSDB(
    override val dao: TSDao[Iterator, Long],
    val metricsDao: TsdbQueryMetricsDao,
    override val dictionaryProvider: DictionaryProvider,
    override val prepareQuery: Query => Query,
    override val extractBatchSize: Int = 10000,
    config: TSDBConfig = TSDBConfig()
) extends TsdbBase
    with StrictLogging {

  override type Collection[X] = Iterator[X]
  override type Result = TsdbServerResult

  private var catalogs = Map.empty[ExternalLink, ExternalLinkService[_ <: ExternalLink]]

  override val mr: MapReducible[Iterator] = MapReducible.iteratorMR

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit = {
    catalogs += (catalog -> catalogService)
  }

  def put(dataPoints: Seq[DataPoint]): Unit = {
    loadTagsIds(dataPoints)
    dao.put(dataPoints)
  }

  override def createMetricCollector(query: Query): MetricQueryCollector = {
    if (config.collectMetrics) {
      val queryCollectorContext = new QueryCollectorContext(
        metricsDao = () => metricsDao,
        operationName = "query",
        metricsUpdateInterval = config.metricsUpdateInterval
      )
      new PersistentMetricQueryCollector(queryCollectorContext, query)
    } else NoMetricCollector
  }

  override def finalizeQuery(
      queryContext: QueryContext,
      data: Iterator[Array[Option[Any]]],
      metricCollector: MetricQueryCollector
  ): TsdbServerResult = {
    val it = new AbstractIterator[Array[Option[Any]]] {
      var hasEnded = false

      override def hasNext: Boolean = {
        val n = data.hasNext
        if (!n && !hasEnded) {
          hasEnded = true
          metricCollector.finish()
          // TODO: Get statistics somehow
          //          logger.trace(s"${queryContext.query.uuidLog}, End query. Processed rows: $processedRows, " +
          //            s"dataPoints: $processedDataPoints, resultRows: $resultRows, " +
          //            s"time: ${System.currentTimeMillis() - startProcessingTime}")
        }
        n
      }

      override def next(): Array[Option[Any]] = data.next()
    }

    new TsdbServerResult(queryContext, it)
  }

  override def applyWindowFunctions(
      queryContext: QueryContext,
      keysAndValues: Iterator[(KeyData, InternalRow)]
  ): Iterator[(KeyData, InternalRow)] = {
    val seq = keysAndValues.zipWithIndex.toList

    val grouped = seq
      .groupBy(_._1._1)
      .map {
        case (keyData, group) =>
          val (values, rowNumbers) = group
            .map { case ((_, valuedata), rowNumber) => (valuedata, rowNumber) }
            .toArray
            .sortBy(_._1.get[Time](queryContext, TimeExpr))
            .unzip

          keyData -> ((values, rowNumbers.zipWithIndex.toMap))
      }

    val winFieldsAndGroupValues = queryContext.query.fields.map(_.expr).collect {
      case winFuncExpr: WindowFunctionExpr =>
        val values = grouped.mapValues {
          case (vs, rowNumIndex) =>
            val funcValues = vs.map(_.get[winFuncExpr.expr.Out](queryContext, winFuncExpr.expr))
            (funcValues, rowNumIndex)
        }
        winFuncExpr -> values
    }

    seq.map {
      case ((keyData, valueData), rowNumber) =>
        winFieldsAndGroupValues.foreach {
          case (winFuncExpr, groups) =>
            val (group, rowIndex) = groups(keyData)
            rowIndex.get(rowNumber).map { index =>
              val value = winFuncExpr.operation(group.asInstanceOf[Array[Option[winFuncExpr.expr.Out]]], index)
              valueData.set(queryContext, winFuncExpr, value)
            }
        }
        keyData -> valueData
    }.toIterator
  }

  def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit = {
    if (statuses.nonEmpty) {
      dao.putRollupStatuses(statuses, table)
    }
  }

  def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = {
    dao.getRollupSpecialField(fieldName, table)
  }

  private def loadTagsIds(dataPoints: Seq[DataPoint]): Unit = {
    dataPoints.groupBy(_.table).foreach {
      case (table, points) =>
        table.dimensionSeq.map { tag =>
          val values = points.flatMap { dp =>
            dp.dimensions.get(tag).filter(_.trim.nonEmpty)
          }
          dictionary(tag).findIdsByValues(values.toSet)
        }
    }
  }

  override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    catalogs.getOrElse(catalog, throw new Exception(s"Can't find catalog ${catalog.linkName}: ${catalog.fieldsNames}"))
  }
}

case class TSDBConfig(collectMetrics: Boolean = false, metricsUpdateInterval: Int = 30000)
