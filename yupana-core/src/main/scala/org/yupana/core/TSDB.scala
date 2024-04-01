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
import org.yupana.api.schema.{ ExternalLink, Schema }
import org.yupana.api.utils.CloseableIterator
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.jit.{ CachingExpressionCalculatorFactory, ExpressionCalculatorFactory }
import org.yupana.core.model.{ InternalRow, InternalRowBuilder, KeyData }
import org.yupana.core.utils.metric._

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

  override def createMetricCollector(query: Query, user: YupanaUser): MetricQueryCollector =
    if (config.collectMetrics) metricCollectorCreator(query, user.name)
    else NoMetricCollector

  override def finalizeQuery(
      queryContext: QueryContext,
      rowBuilder: InternalRowBuilder,
      data: Iterator[InternalRow],
      metricCollector: MetricQueryCollector
  ): TsdbServerResult = {
    val it = CloseableIterator(data, metricCollector.finish())
    new TsdbServerResult(queryContext, rowBuilder, it)
  }

  override def applyWindowFunctions(
      queryContext: QueryContext,
      internalRowBuilder: InternalRowBuilder,
      keysAndValues: Iterator[(KeyData, InternalRow)]
  ): Iterator[(KeyData, InternalRow)] = {
    val seq = keysAndValues.zipWithIndex.toList

    val grouped = seq
      .groupBy(_._1._1)
      .map {
        case (keyData, group) =>
          val (values, rowNumbers) = group
            .map { case ((_, row), rowNumber) => (row, rowNumber) }
            .toArray
            .sortBy(_._1.get[Time](internalRowBuilder, TimeExpr))
            .unzip

          keyData -> ((values, rowNumbers.zipWithIndex.toMap))
      }

    val winFieldsAndGroupValues = queryContext.query.fields.map(_.expr).collect {
      case winFuncExpr: WindowFunctionExpr[_, _] =>
        val values = grouped.map {
          case (key, (vs, rowNumIndex)) =>
            val funcValues = winFuncExpr.expr.dataType.classTag.newArray(vs.length)
            vs.indices.foreach { i =>
              funcValues(i) = vs(i).get(internalRowBuilder, winFuncExpr.expr)
            }
            (key, (funcValues, rowNumIndex))
        }
        winFuncExpr -> values
    }

    seq.map {
      case ((keyData, row), rowNumber) =>
        internalRowBuilder.setFieldsFromRow(row)
        winFieldsAndGroupValues.foreach {
          case (winFuncExpr, groups) =>
            val (group, rowIndex) = groups(keyData)
            rowIndex.get(rowNumber).map { index =>
              val value = queryContext.calculator.evaluateWindow(winFuncExpr, group, index)
              if (value != null) {
                internalRowBuilder.set(winFuncExpr, value)
              } else {
                internalRowBuilder.setNull(winFuncExpr)
              }
            }
        }
        keyData -> internalRowBuilder.buildAndReset()
    }.iterator
  }

  override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    externalLinks.getOrElse(
      catalog,
      throw new Exception(s"Can't find catalog ${catalog.linkName}: ${catalog.fields}")
    )
  }

  override def externalLinkServices: Iterable[ExternalLinkService[_]] = externalLinks.values
}
