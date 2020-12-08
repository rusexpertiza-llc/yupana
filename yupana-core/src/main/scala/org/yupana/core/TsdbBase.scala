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
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, ExternalLink, Schema }
import org.yupana.core.dao.{ DictionaryProvider, TSReadingDao }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, KeyData }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.{ CollectionUtils, ConditionUtils }

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

  // TODO: it should work with different DAO Id types
  def dao: TSReadingDao[Collection, Long]

  def mapReduceEngine(metricCollector: MetricQueryCollector): MapReducible[Collection] =
    dao.mapReduceEngine(metricCollector)

  def dictionaryProvider: DictionaryProvider

  def schema: Schema

  lazy val expressionCalculator: ExpressionCalculator = new ExpressionCalculator(schema.tokenizer)

  /** Batch size for reading values from external links */
  val extractBatchSize: Int

  def dictionary(dimension: DictionaryDimension): Dictionary = dictionaryProvider.dictionary(dimension)

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
      rows: Collection[Array[Any]],
      metricCollector: MetricQueryCollector
  ): Result

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

    val optimizedQuery = QueryOptimizer.optimize(expressionCalculator)(preparedQuery)

    logger.debug(s"Optimized query: $optimizedQuery")

    val metricCollector = createMetricCollector(optimizedQuery)

    val substitutedCondition = optimizedQuery.filter.map(c => substituteLinks(c, metricCollector))
    logger.debug(s"Substituted condition: $substitutedCondition")

    val condition = substitutedCondition.map(c => ConditionUtils.split(c)(dao.isSupportedCondition)._2)

    logger.debug(s"Final condition: $condition")

    val queryContext = QueryContext(optimizedQuery, condition)

    val mr = mapReduceEngine(metricCollector)

    val rows = queryContext.query.table match {
      case Some(table) =>
        val daoExprs = queryContext.bottomExprs.collect {
          case e: DimensionExpr[_] => e
          case e: DimensionIdExpr  => e
          case e: MetricExpr[_]    => e
          case TimeExpr            => TimeExpr
        }

        substitutedCondition match {
          case Some(c) =>
            val internalQuery = InternalQuery(table, daoExprs.toSet, c)
            dao.query(internalQuery, new InternalRowBuilder(queryContext), metricCollector)

          case None =>
            throw new IllegalArgumentException("Empty condition")
        }
      case None =>
        val rb = new InternalRowBuilder(queryContext)
        mr.singleton(rb.buildAndReset())
    }
    val processedRows = new AtomicInteger(0)
    val processedDataPoints = new AtomicInteger(0)

    val resultRows = new AtomicInteger(0)

    val isWindowFunctionPresent = queryContext.query.fields.exists(_.expr.kind == Window)

    val keysAndValues = mr.batchFlatMap(rows, extractBatchSize) { batch =>
      val batchSize = batch.size
      val c = processedRows.incrementAndGet()
      if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Fetched $c rows")
      val withExtLinks = metricCollector.readExternalLinks.measure(batchSize) {
        readExternalLinks(queryContext, batch)
      }

      metricCollector.extractDataComputation.measure(batchSize) {
        val it = withExtLinks.iterator
        val filtered = condition match {
          case Some(cond) =>
            val withValuesForFilter = it.map(row => evaluateFilterExprs(queryContext, cond, row))
            withValuesForFilter.filter(row => expressionCalculator.preEvaluated(cond, queryContext, row))
          case None => it
        }

        val withExprValues = filtered.map(row => evaluateExpressions(queryContext, row))

        withExprValues.map(row => new KeyData(queryContext, row) -> row)
      }
    }

    val keysAndValuesWinFunc = if (isWindowFunctionPresent) {
      metricCollector.windowFunctions.measure(1) {
        applyWindowFunctions(queryContext, keysAndValues)
      }
    } else {
      keysAndValues
    }

    val reduced = if (queryContext.query.groupBy.nonEmpty && !isWindowFunctionPresent) {
      val keysAndMappedValues = mr.batchFlatMap(keysAndValuesWinFunc, extractBatchSize) { batch =>
        metricCollector.reduceOperation.measure(batch.size) {
          val mapped = batch.iterator.map {
            case (key, row) =>
              val c = processedDataPoints.incrementAndGet()
              if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Extracted $c data points")
              key -> applyMapOperation(queryContext, row)
          }
          CollectionUtils.reduceByKey(mapped)((a, b) => applyReduceOperation(queryContext, a, b))
        }
      }

      val r = mr.reduceByKey(keysAndMappedValues) { (a, b) =>
        metricCollector.reduceOperation.measure(1) {
          applyReduceOperation(queryContext, a, b)
        }
      }

      mr.batchFlatMap(r, extractBatchSize) { batch =>
        metricCollector.reduceOperation.measure(batch.size) {
          val it = batch.iterator
          it.map {
            case (key, row) =>
              applyPostMapOperation(queryContext, row)
          }
        }
      }
    } else {
      mr.map(keysAndValuesWinFunc)(_._2)
    }

    val calculated = mr.map(reduced) { row =>
      evalExprsOnAggregatesAndWindows(queryContext, row)
    }

    val postFiltered = queryContext.query.postFilter match {
      case Some(cond) =>
        mr.batchFlatMap(calculated, extractBatchSize) { batch =>
          metricCollector.postFilter.measure(batch.size) {
            val it = batch.iterator
            it.filter { row =>
              expressionCalculator.preEvaluated(cond, queryContext, row)
            }
          }
        }
      case None => calculated
    }

    val limited = queryContext.query.limit.map(mr.limit(postFiltered)).getOrElse(postFiltered)

    val result = mr.batchFlatMap(limited, extractBatchSize) { batch =>
      metricCollector.collectResultRows.measure(batch.size) {
        batch.iterator.map { row =>
          val c = resultRows.incrementAndGet()
          val d = if (c <= 100000) 10000 else 100000
          if (c % d == 0) {
            logger.trace(s"${queryContext.query.uuidLog} -- Created $c result rows")
          }
          row.data
        }
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
      postCondition: Expression.Condition,
      row: InternalRow
  ): InternalRow = {
    if (postCondition.kind != Const) {
      row.set(
        queryContext.exprsIndex(postCondition),
        expressionCalculator.evaluateExpression(postCondition, queryContext, row)
      )
    }
    row
  }

  def evaluateExpressions(
      queryContext: QueryContext,
      row: InternalRow
  ): InternalRow = {
    queryContext.bottomExprs.foreach { expr =>
      row.set(
        queryContext.exprsIndex(expr),
        expressionCalculator.evaluateExpression(expr, queryContext, row)
      )
    }

    queryContext.topRowExprs.foreach { expr =>
      row.set(
        queryContext.exprsIndex(expr),
        expressionCalculator.evaluateExpression(expr, queryContext, row)
      )
    }

    row
  }

  def applyMapOperation(queryContext: QueryContext, row: InternalRow): InternalRow = {
    queryContext.aggregateExprs.foreach { ae =>
      row.set(
        queryContext.exprsIndex(ae),
        expressionCalculator.evaluateMap(ae, queryContext, row)
      )
    }
    row
  }

  def applyReduceOperation(queryContext: QueryContext, a: InternalRow, b: InternalRow): InternalRow = {
    val reduced = a.copy
    queryContext.aggregateExprs.foreach { aggExpr =>
      val newValue = expressionCalculator.evaluateReduce(aggExpr, queryContext, a, b)
      reduced.set(queryContext, aggExpr, newValue)
    }

    reduced
  }

  def applyPostMapOperation(queryContext: QueryContext, row: InternalRow): InternalRow = {

    queryContext.aggregateExprs.foreach { aggExpr =>
      row.set(queryContext, aggExpr, expressionCalculator.evaluatePostMap(aggExpr, queryContext, row))
    }
    row
  }

  def evalExprsOnAggregatesAndWindows(queryContext: QueryContext, data: InternalRow): InternalRow = {
    queryContext.exprsOnAggregatesAndWindows.foreach { e =>
      val nullWindowExpressionsExists = e.flatten.exists {
        case w: WindowFunctionExpr[_, _] => data.isEmpty(queryContext, w)
        case _                           => false
      }
      val evaluationResult =
        if (nullWindowExpressionsExists) null
        else expressionCalculator.evaluateExpression(e, queryContext, data)
      data.set(queryContext, e, evaluationResult)
    }
    data
  }

  def substituteLinks(condition: Condition, metricCollector: MetricQueryCollector): Condition = {
    val linkServices = condition.flatten.collect {
      case LinkExpr(c, _) => linkService(c)
    }

    val substituted = linkServices.map(service =>
      metricCollector.dynamicMetric(s"create_queries.link.${service.externalLink.linkName}").measure(1) {
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
