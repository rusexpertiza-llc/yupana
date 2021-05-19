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
import org.yupana.api.schema.{ DictionaryDimension, ExternalLink, Schema, Table }
import org.yupana.core.dao.{ DictionaryProvider, TSDao, TsdbQueryMetricsDao }
import org.yupana.core.model.{ InternalRow, KeyData }
import org.yupana.core.utils.CloseableIterator
import org.yupana.core.utils.metric._

class TSDB(
    override val schema: Schema,
    override val dao: TSDao[Iterator, Long],
    override val dictionaryProvider: DictionaryProvider,
    override val prepareQuery: Query => Query,
    config: TsdbConfig,
    metricCollectorCreator: Query => MetricQueryCollector
) extends TsdbBase
    with StrictLogging {

  override type Collection[X] = Iterator[X]
  override type Result = TsdbServerResult

  override lazy val extractBatchSize: Int = config.extractBatchSize

  private var externalLinks = Map.empty[ExternalLink, ExternalLinkService[_ <: ExternalLink]]

  def registerExternalLink(
      externalLink: ExternalLink,
      externalLinkService: ExternalLinkService[_ <: ExternalLink]
  ): Unit = {
    externalLinks += (externalLink -> externalLinkService)
  }

  def put(dataPoints: Seq[DataPoint]): Unit = {
    if (config.putEnabled) {
      loadDimIds(dataPoints)
      dao.put(dataPoints)
      externalLinks.foreach(_._2.put(dataPoints))
    } else throw new IllegalAccessException("Put is disabled")
  }

  override def createMetricCollector(query: Query): MetricQueryCollector = {
    if (config.collectMetrics) {
      metricCollectorCreator(query)
    } else NoMetricCollector
  }

  override def finalizeQuery(
      queryContext: QueryContext,
      data: Iterator[Array[Any]],
      metricCollector: MetricQueryCollector
  ): TsdbServerResult = {

    val it = CloseableIterator(data, metricCollector.finish)
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
      case winFuncExpr: WindowFunctionExpr[_, _] =>
        val values = grouped.mapValues {
          case (vs, rowNumIndex) =>
            val funcValues = winFuncExpr.expr.dataType.classTag.newArray(vs.length)
            vs.indices.foreach { i =>
              funcValues(i) = vs(i).get(queryContext, winFuncExpr.expr)
            }
            (funcValues, rowNumIndex)
        }
        winFuncExpr -> values
    }

    seq.map {
      case ((keyData, valueData), rowNumber) =>
        winFieldsAndGroupValues.foreach {
          case (winFuncExpr: WindowFunctionExpr[t, _], groups) =>
            val (group, rowIndex) = groups(keyData)
            rowIndex.get(rowNumber).map { index =>
              val value = expressionCalculator.evaluateWindow(winFuncExpr, group.asInstanceOf[Array[t]], index)
              valueData.set(queryContext, winFuncExpr, value)
            }
        }
        keyData -> valueData
    }.toIterator
  }

  private def loadDimIds(dataPoints: Seq[DataPoint]): Unit = {
    dataPoints.groupBy(_.table).foreach {
      case (table, points) =>
        table.dimensionSeq.foreach {
          case dimension: DictionaryDimension =>
            val values = points.flatMap { dp =>
              dp.dimensionValue(dimension).filter(_.trim.nonEmpty)
            }
            dictionary(dimension).findIdsByValues(values.toSet)

          case _ =>
        }
    }
  }

  override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    externalLinks.getOrElse(
      catalog,
      throw new Exception(s"Can't find catalog ${catalog.linkName}: ${catalog.fields}")
    )
  }
}
