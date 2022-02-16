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
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, ExternalLink, Schema }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.{ ChangelogDao, DictionaryProvider, TSDao }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, KeyData }
import org.yupana.core.utils.metric.{ Failed, MetricQueryCollector, NoMetricCollector }
import org.yupana.core.utils.{ CollectionUtils, ConditionUtils, TimeBoundedCondition }

import java.util.concurrent.atomic.AtomicInteger

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
  def dao: TSDao[Collection, Long]

  def changelogDao: ChangelogDao

  def mapReduceEngine(metricCollector: MetricQueryCollector): MapReducible[Collection] =
    dao.mapReduceEngine(metricCollector)

  def dictionaryProvider: DictionaryProvider

  def schema: Schema

  private lazy val constantCalculator: ConstantCalculator = new ConstantCalculator(schema.tokenizer)

  /** Batch size for reading values from external links */
  val extractBatchSize: Int

  /** Batch size for writing values to external links */
  val putBatchSize: Int

  def dictionary(dimension: DictionaryDimension): Dictionary = dictionaryProvider.dictionary(dimension)

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit

  def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink]
  def externalLinkServices: Iterable[ExternalLinkService[_]]

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

    val optimizedQuery = QueryOptimizer.optimize(constantCalculator)(preparedQuery)

    logger.debug(s"Optimized query: $optimizedQuery")

    val metricCollector = createMetricCollector(optimizedQuery)
    val mr = mapReduceEngine(metricCollector)

    metricCollector.start()

    val substitutedCondition = optimizedQuery.filter.map(c => substituteLinks(c, metricCollector))
    logger.debug(s"Substituted condition: $substitutedCondition")

    val condition = substitutedCondition
      .map(c => ConditionUtils.split(c)(dao.isSupportedCondition)._2)
      .filterNot(_ == ConstantExpr(true))

    logger.debug(s"Final condition: $condition")

    val queryContext = QueryContext(optimizedQuery, condition)

    val rows = queryContext.query.table match {
      case Some(table) =>
        val daoExprs = queryContext.exprsIndex.keys.collect {
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
            val th = new IllegalArgumentException("Empty condition")
            metricCollector.queryStatus.set(Failed(th))
            throw th
        }
      case None =>
        val rb = new InternalRowBuilder(queryContext)
        mr.singleton(rb.buildAndReset())
    }

    processRows(queryContext, metricCollector, mr, rows)
  }

  def processRows(
      queryContext: QueryContext,
      metricCollector: MetricQueryCollector,
      mr: MapReducible[Collection],
      rows: Collection[InternalRow]
  ): Result = {
    val processedRows = new AtomicInteger(0)
    val processedDataPoints = new AtomicInteger(0)
    val resultRows = new AtomicInteger(0)

    val hasWindowFunctions = queryContext.query.fields.exists(_.expr.kind == Window)
    val hasAggregates = queryContext.query.fields.exists(_.expr.kind == Aggregate)
    val keysAndValues = mr.batchFlatMap(rows, extractBatchSize) { batch =>
      val batchSize = batch.size
      val c = processedRows.incrementAndGet()
      if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Fetched $c rows")
      val withExtLinks = metricCollector.readExternalLinks.measure(batchSize) {
        readExternalLinks(queryContext, batch)
      }

      metricCollector.extractDataComputation.measure(batchSize) {
        val it = withExtLinks.iterator
        val filtered = queryContext.postCondition match {
          case Some(_) =>
            it.filter(row => queryContext.calculator.evaluateFilter(schema.tokenizer, row))
          case None => it
        }

        val withExprValues = filtered.map(row => queryContext.calculator.evaluateExpressions(schema.tokenizer, row))

        withExprValues.map(row => new KeyData(queryContext, row) -> row)
      }
    }

    val keysAndValuesWinFunc = if (hasWindowFunctions) {
      metricCollector.windowFunctions.measure(1) {
        applyWindowFunctions(queryContext, keysAndValues)
      }
    } else {
      keysAndValues
    }

    val reduced = if (hasAggregates && !hasWindowFunctions) {
      val keysAndMappedValues = mr.batchFlatMap(keysAndValuesWinFunc, extractBatchSize) { batch =>
        metricCollector.reduceOperation.measure(batch.size) {
          val mapped = batch.iterator.map {
            case (key, row) =>
              val c = processedDataPoints.incrementAndGet()
              if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Extracted $c data points")
              key -> queryContext.calculator.evaluateMap(schema.tokenizer, row)
          }
          CollectionUtils.reduceByKey(mapped)((a, b) => queryContext.calculator.evaluateReduce(schema.tokenizer, a, b))
        }
      }

      val r = mr.reduceByKey(keysAndMappedValues) { (a, b) =>
        metricCollector.reduceOperation.measure(1) {
          queryContext.calculator.evaluateReduce(schema.tokenizer, a, b)
        }
      }

      mr.batchFlatMap(r, extractBatchSize) { batch =>
        metricCollector.reduceOperation.measure(batch.size) {
          val it = batch.iterator
          it.map {
            case (_, row) =>
              queryContext.calculator.evaluatePostMap(schema.tokenizer, row)
          }
        }
      }
    } else {
      mr.map(keysAndValuesWinFunc)(_._2)
    }

    val calculated = mr.map(reduced) { row =>
      queryContext.calculator.evaluatePostAggregateExprs(schema.tokenizer, row)
    }

    val postFiltered = queryContext.query.postFilter match {
      case Some(_) =>
        mr.batchFlatMap(calculated, extractBatchSize) { batch =>
          metricCollector.postFilter.measure(batch.size) {
            val it = batch.iterator
            it.filter(row => queryContext.calculator.evaluatePostFilter(schema.tokenizer, row))
          }
        }
      case None => calculated
    }

    val limited = queryContext.query.limit.map(mr.limit(postFiltered)).getOrElse(postFiltered)

    val result = mr
      .batchFlatMap(limited, extractBatchSize) { batch =>
        metricCollector.collectResultRows.measure(batch.size) {
          batch.iterator.map { row =>
            {
              val c = resultRows.incrementAndGet()
              val d = if (c <= 100000) 10000 else 100000
              if (c % d == 0) {
                logger.trace(s"${queryContext.query.uuidLog} -- Created $c result rows")
              }
              row.data
            }
          }
        }
      }

    finalizeQuery(queryContext, result, metricCollector)
  }

  def readExternalLinks(queryContext: QueryContext, rows: Seq[InternalRow]): Seq[InternalRow] = {
    queryContext.linkExprs.groupBy(_.link).foreach {
      case (c, exprs) =>
        val externalLink = linkService(c)
        externalLink.setLinkedValues(queryContext.exprsIndex, rows, exprs.toSet)
    }
    rows
  }

  def substituteLinks(condition: Condition, metricCollector: MetricQueryCollector): Condition = {
    val linkServices = condition.flatten.collect {
      case LinkExpr(c, _) => linkService(c)
    }

    val transformations = linkServices.flatMap(service =>
      metricCollector.dynamicMetric(s"create_queries.link.${service.externalLink.linkName}").measure(1) {
        service.transformCondition(condition)
      }
    )

    if (transformations.nonEmpty) {
      val tbc = TimeBoundedCondition.single(constantCalculator, condition)
      val transformed = transformations.foldLeft(tbc) {
        case (c, transform) =>
          ConditionUtils.transform(c, transform)
      }
      transformed.toCondition
    } else {
      condition
    }
  }

  def put(dataPoints: Collection[DataPoint], user: YupanaUser = YupanaUser.ANONYMOUS): Unit = {
    val mr = mapReduceEngine(NoMetricCollector)
    val withExternalLinks = mr.batchFlatMap(dataPoints, putBatchSize) { seq =>
      externalLinkServices.foreach(_.put(seq))
      seq
    }
    val updatedIntervals = dao.put(mr, withExternalLinks, user.name)

    changelogDao.putUpdatesIntervals(updatedIntervals)
  }
}
