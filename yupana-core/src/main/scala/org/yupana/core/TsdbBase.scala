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

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, ExternalLink }
import org.yupana.core.dao.{ DictionaryProvider, TSReadingDao }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, KeyData }
import org.yupana.core.operations.Operations
import org.yupana.core.utils.ConditionUtils
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.language.higherKinds

/**
  * Core of time series database processing pipeline.
  */
trait TsdbBase extends StrictLogging {

  /**
    * Type of collection used in this TSDB instance and the DAO. The default implementation uses Iterator as a collection type.
    * Spark based implementation uses RDD.
    */
  type Collection[_]
  type Result <: TsdbResultBase[Collection]

  def mr: MapReducible[Collection]

  // TODO: it should work with different DAO Id types
  def dao: TSReadingDao[Collection, Long]

  def dictionaryProvider: DictionaryProvider

  /** Batch size for reading values from external links */
  val extractBatchSize: Int

  def dictionary(dimension: Dimension): Dictionary = dictionaryProvider.dictionary(dimension)

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit

  def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink]

  def prepareQuery: Query => Query

  def applyWindowFunctions(
      queryContext: QueryContext,
      keysAndValues: Collection[(KeyData, InternalRow)]
  ): Collection[(KeyData, InternalRow)]

  def createMetricCollector(query: Query): MetricQueryCollector

  def finalizeQuery(
      queryContext: QueryContext,
      rows: Collection[Array[Option[Any]]],
      metricCollector: MetricQueryCollector
  ): Result

  implicit protected val operations: Operations = Operations

  /**
    * Query pipeline. Perform following stages:
    *
    * - creates queries for DAO
    * - call DAO query to get [[Collection]] of rows
    * - fills the rows with external links values
    * - extract KeyData and ValueData
    * - apply value filters
    * - window function application
    * - apply aggregation: map, reduce, post-map
    * - post reduce arithmetics
    * - extract field values
    *
    * The pipeline is not responsible for limiting. This means that collection have to be lazy, to avoid extra
    * calculations if limit is defined.
    */
  def query(query: Query): Result = {

    val preparedQuery = prepareQuery(query)
    logger.info(s"TSDB query with ${preparedQuery.uuidLog} start: " + preparedQuery)

    val metricCollector = createMetricCollector(preparedQuery)

    val simplified = ConditionUtils.simplify(preparedQuery.filter)

    val substitutedCondition = substituteLinks(simplified, metricCollector)
    logger.debug(s"Substituted condition: $substitutedCondition")

    val postCondition = ConditionUtils.split(substitutedCondition)(dao.isSupportedCondition)._2

    logger.debug(s"Post condition: $postCondition")

    val queryContext = QueryContext(preparedQuery, postCondition)

    val daoExprs = queryContext.bottomExprs.collect {
      case e: DimensionExpr => e
      case e: MetricExpr[_] => e
      case TimeExpr         => TimeExpr
    }

    val internalQuery = InternalQuery(queryContext.query.table, daoExprs.toSet, substitutedCondition)

    val rows = dao.query(internalQuery, new InternalRowBuilder(queryContext), metricCollector)

    val processedRows = new AtomicInteger(0)
    val processedDataPoints = new AtomicInteger(0)
    val resultRows = new AtomicInteger(0)

    val withExternalFields = mr.batchFlatMap(rows)(
      extractBatchSize,
      values => {
        val c = processedRows.incrementAndGet()
        if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Fetched $c tsd rows")
        readExternalLinks(queryContext, values)
      }
    )

    val filterValuesEvaluated =
      mr.map(withExternalFields)(values => evaluateFilterExprs(queryContext, values, metricCollector))

    val valuesFiltered = queryContext.postCondition
      .map(
        c =>
          mr.filter(filterValuesEvaluated)(
            values => ExpressionCalculator.evaluateCondition(c, queryContext, values).getOrElse(false)
          )
      )
      .getOrElse(filterValuesEvaluated)

    val valuesEvaluated = mr.map(valuesFiltered)(values => evaluateExpressions(queryContext, values, metricCollector))

    val keysAndValues = mr.map(valuesEvaluated)(row => new KeyData(queryContext, row) -> row)

    val isWindowFunctionPresent = metricCollector.windowFunctionsCheck.measure {
      queryContext.query.fields.exists(_.expr.isInstanceOf[WindowFunctionExpr])
    }

    val keysAndValuesWinFunc = if (isWindowFunctionPresent) {
      metricCollector.windowFunctions.measure {
        applyWindowFunctions(queryContext, keysAndValues)
      }
    } else {
      keysAndValues
    }

    val reduced = if (queryContext.query.groupBy.nonEmpty && !isWindowFunctionPresent) {
      val keysAndMappedValues = mr.map(keysAndValuesWinFunc) {
        case (key, values) =>
          key -> metricCollector.mapOperation.measure {
            val c = processedDataPoints.incrementAndGet()
            if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Extracted $c data points")

            applyMapOperation(queryContext, values)
          }
      }

      val r = mr.reduceByKey(keysAndMappedValues)(
        (a, b) =>
          metricCollector.reduceOperation.measure {
            applyReduceOperation(queryContext, a, b)
          }
      )

      mr.map(r)(
        kv =>
          metricCollector.postMapOperation.measure {
            kv._1 -> applyPostMapOperation(queryContext, kv._2)
          }
      )
    } else {
      keysAndValuesWinFunc
    }

    val calculated = mr.map(reduced) {
      case (k, v) => (k, evalExprsOnAggregatesAndWindows(queryContext, v))
    }

    val postFiltered = queryContext.query.postFilter
      .map(
        c =>
          metricCollector.postFilter.measure {
            mr.filter(calculated)(kv => ExpressionCalculator.evaluateCondition(c, queryContext, kv._2).getOrElse(false))
          }
      )
      .getOrElse(calculated)

    val limited = queryContext.query.limit.map(mr.limit(postFiltered)).getOrElse(postFiltered)

    val result = mr.map(limited) {
      case (_, valueData) =>
        metricCollector.collectResultRows.measure {
          val c = resultRows.incrementAndGet()
          val d = if (c <= 100000) 10000 else 100000
          if (c % d == 0) {
            logger.trace(s"${queryContext.query.uuidLog} -- Created $c result rows")
          }
          valueData.data
        }
    }

    finalizeQuery(queryContext, result, metricCollector)
  }

  def readExternalLinks(queryContext: QueryContext, rows: Seq[InternalRow]): Seq[InternalRow] = {
    queryContext.linkExprs.groupBy(_.link).foreach {
      case (c, exprs) =>
        val catalog = linkService(c)
        catalog.setLinkedValues(queryContext.exprsIndex, rows, exprs.toSet)
    }

    rows
  }

  def evaluateFilterExprs(
      queryContext: QueryContext,
      row: InternalRow,
      metricCollector: MetricQueryCollector
  ): InternalRow = {
    metricCollector.extractDataComputation.measure {
      queryContext.postConditionExprs.foreach { expr =>
        row.set(
          queryContext.exprsIndex(expr),
          ExpressionCalculator.evaluateExpression(expr, queryContext, row)
        )
      }
    }

    row
  }

  def evaluateExpressions(
      queryContext: QueryContext,
      row: InternalRow,
      metricCollector: MetricQueryCollector
  ): InternalRow = {
    metricCollector.extractDataComputation.measure {
      queryContext.bottomExprs.foreach { expr =>
        row.set(
          queryContext.exprsIndex(expr),
          ExpressionCalculator.evaluateExpression(expr, queryContext, row)
        )
      }

      queryContext.topRowExprs.foreach { expr =>
        row.set(
          queryContext.exprsIndex(expr),
          ExpressionCalculator.evaluateExpression(expr, queryContext, row)
        )
      }
    }

    row
  }

  def applyMapOperation(queryContext: QueryContext, values: InternalRow): InternalRow = {
    queryContext.aggregateExprs.foreach { ae =>
      val oldValue = values.get[ae.expr.Out](queryContext, ae.expr)
      val newValue = oldValue.map(v => ae.aggregation.map(v))
      values.set(queryContext, ae, newValue)
    }
    values
  }

  def applyReduceOperation(queryContext: QueryContext, a: InternalRow, b: InternalRow): InternalRow = {
    val reduced = a.copy
    queryContext.aggregateExprs.foreach { aggExpr =>
      val agg = aggExpr.aggregation
      val aValue = a.get[agg.Interim](queryContext, aggExpr)
      val bValue = b.get[agg.Interim](queryContext, aggExpr)

      val newValue = aValue match {
        case Some(av) =>
          bValue.map(bv => agg.reduce(av, bv)).orElse(aValue)
        case None => bValue
      }
      reduced.set(queryContext, aggExpr, newValue)
    }

    reduced
  }

  def applyPostMapOperation(queryContext: QueryContext, data: InternalRow): InternalRow = {

    queryContext.aggregateExprs.foreach { aggExpr =>
      val agg = aggExpr.aggregation
      val newValue = data.get[agg.Interim](queryContext, aggExpr).map(agg.postMap)
      data.set(queryContext, aggExpr, newValue)
    }
    data
  }

  def evalExprsOnAggregatesAndWindows(queryContext: QueryContext, data: InternalRow): InternalRow = {
    queryContext.exprsOnAggregatesAndWindows.foreach { e =>
      val nullWindowExpressionsExists = e.flatten.exists {
        case w: WindowFunctionExpr => data.get(queryContext, w).isEmpty
        case _                     => false
      }
      val evaluationResult =
        if (nullWindowExpressionsExists) None
        else ExpressionCalculator.evaluateExpression(e, queryContext, data)
      data.set(queryContext, e, evaluationResult)
    }
    data
  }

  def substituteLinks(condition: Condition, metricCollector: MetricQueryCollector): Condition = {
    val linkServices = condition.exprs.flatMap(_.flatten).collect {
      case LinkExpr(c, _) => linkService(c)
    }

    val substituted = linkServices.map(
      service =>
        metricCollector.dynamicMetric(s"create_queries.link.${service.externalLink.linkName}").measure {
          service.condition(condition)
        }
    )

    if (substituted.nonEmpty) {
      val merged = substituted.reduceLeft(ConditionUtils.merge)
      ConditionUtils.split(merged)(c => linkServices.exists(_.isSupportedCondition(c)))._2
    } else {
      condition
    }
  }
}
