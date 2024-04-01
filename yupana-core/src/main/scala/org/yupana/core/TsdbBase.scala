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
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ ExternalLink, Schema }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.jit.ExpressionCalculatorFactory
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, KeyData }
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.core.utils.{ ConditionUtils, FlatAndCondition }
import org.yupana.metrics.Failed

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

  def schema: Schema

  def calculatorFactory: ExpressionCalculatorFactory

  private lazy val constantCalculator: ConstantCalculator = new ConstantCalculator(schema.tokenizer)

  /** Batch size for reading values from external links */
  val extractBatchSize: Int

  /** Batch size for writing values to external links */
  val putBatchSize: Int

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit

  def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink]
  def externalLinkServices: Iterable[ExternalLinkService[_]]

  def prepareQuery: Query => Query

  def applyWindowFunctions(
      queryContext: QueryContext,
      internalRowBuilder: InternalRowBuilder,
      keysAndValues: Collection[(KeyData, InternalRow)]
  ): Collection[(KeyData, InternalRow)]

  def createMetricCollector(query: Query, user: YupanaUser): MetricQueryCollector

  def finalizeQuery(
      queryContext: QueryContext,
      rowBuilder: InternalRowBuilder,
      rows: Collection[InternalRow],
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
  def query(query: Query, user: YupanaUser = YupanaUser.ANONYMOUS): Result = {

    val preparedQuery = prepareQuery(query)
    logger.info(s"User ${user.name} start TSDB query with ${preparedQuery.uuidLog} start: " + preparedQuery)

    val optimizedQuery = QueryOptimizer.optimize(constantCalculator)(preparedQuery)

    logger.debug(s"Optimized query: $optimizedQuery")

    val metricCollector = createMetricCollector(optimizedQuery, user)
    val mr = mapReduceEngine(metricCollector)

    metricCollector.start()

    val (rows, queryContext, internalRowBuilder) = query.table match {
      case Some(table) =>
        optimizedQuery.filter match {
          case Some(conditionAsIs) =>
            val flatAndCondition = FlatAndCondition(constantCalculator, conditionAsIs)

            val substitutedCondition = substituteLinks(flatAndCondition, metricCollector)
            logger.debug(s"Substituted condition: $substitutedCondition")

            val postDaoConditions = substitutedCondition.map { tbc =>
              if (FlatAndCondition.mergeByTime(flatAndCondition).flatMap(_._3).distinct.size == 1) {
                logger.debug(s"Same flatAndConditions set for all time bounds")
                tbc.copy(conditions = tbc.conditions.filter { c =>
                  c != ConstantExpr(true) && !dao.isSupportedCondition(c)
                })
              } else {
                logger.debug(s"Different flatAndConditions sets exists for different time bounds")
                tbc
              }
            }

            logger.debug(s"Without dao conditions: $postDaoConditions")

            val finalPostDaoCondition = mergeCondition(postDaoConditions)
            logger.debug(s"Final post condition: $finalPostDaoCondition")

            val qc =
              metricCollector.createContext.measure(1)(
                new QueryContext(optimizedQuery, finalPostDaoCondition, calculatorFactory, metricCollector)
              )

            val rowBuilder = new InternalRowBuilder(qc)

            val daoExprs = qc.exprsIndex.keys.collect {
              case e: DimensionExpr[_] => e
              case e: DimensionIdExpr  => e
              case e: MetricExpr[_]    => e
              case TimeExpr            => TimeExpr
            }

            val internalQuery =
              new InternalQuery(table, daoExprs.toSet[Expression[_]], substitutedCondition, query.hints)

            val rows = dao.query(internalQuery, rowBuilder, qc, metricCollector)

            (rows, qc, rowBuilder)

          case None =>
            val th = new IllegalArgumentException("Empty condition")
            metricCollector.setQueryStatus(Failed(th))
            throw th
        }
      case None =>
        val qc =
          metricCollector.createContext.measure(1)(
            new QueryContext(optimizedQuery, query.filter, calculatorFactory, metricCollector)
          )
        val rb = new InternalRowBuilder(qc)
        val rows = mr.singleton(rb.buildAndReset())
        (rows, qc, rb)
    }

    processRows(queryContext, internalRowBuilder, metricCollector, mr, rows)
  }

  def processRows(
      queryContext: QueryContext,
      rowBuilder: InternalRowBuilder,
      metricCollector: MetricQueryCollector,
      mr: MapReducible[Collection],
      rows: Collection[InternalRow]
  ): Result = {
    val processedRows = new AtomicInteger(0)
    val resultRows = new AtomicInteger(0)

    val hasWindowFunctions = queryContext.query.fields.exists(_.expr.kind == Window)
    val hasAggregates = queryContext.query.fields.exists(_.expr.kind == Aggregate)
    val hasGroupBy = queryContext.query.groupBy.nonEmpty

    val values = mr.batchFlatMap(rows, extractBatchSize) { batch =>
      val batchSize = batch.size
      val c = processedRows.incrementAndGet()
      if (c % 100000 == 0) logger.trace(s"${queryContext.query.uuidLog} -- Fetched $c rows")
      val withExtLinks = metricCollector.readExternalLinks.measure(batchSize) {
        readExternalLinks(queryContext, rowBuilder, batch)
      }

      metricCollector.extractDataComputation.measure(batchSize) {
        val it = withExtLinks.iterator
        val filtered = queryContext.postCondition match {
          case Some(_) =>
            it.filter(row => queryContext.calculator.evaluateFilter(rowBuilder, schema.tokenizer, row))
          case None => it
        }

        filtered.map(row => queryContext.calculator.evaluateExpressions(rowBuilder, schema.tokenizer, row))
      }
    }

    val calculated = if (hasGroupBy || !hasAggregates) {
      val keysAndValues = mr.map(values) { row =>
        new KeyData(queryContext, rowBuilder, row) -> row
      }

      val keysAndValuesWinFunc = if (hasWindowFunctions) {
        metricCollector.windowFunctions.measure(1) {
          applyWindowFunctions(queryContext, rowBuilder, keysAndValues)
        }
      } else {
        keysAndValues
      }

      val reduced = if ((hasAggregates || queryContext.query.groupBy.nonEmpty) && !hasWindowFunctions) {
        val r = mr.aggregateByKey[KeyData, InternalRow, InternalRow](keysAndValuesWinFunc)(
          r => queryContext.calculator.evaluateZero(rowBuilder, schema.tokenizer, r),
          (a, r) => queryContext.calculator.evaluateSequence(rowBuilder, schema.tokenizer, a, r),
          (a, b) => queryContext.calculator.evaluateCombine(rowBuilder, schema.tokenizer, a, b)
        )

        mr.batchFlatMap(r, extractBatchSize) { batch =>
          metricCollector.reduceOperation.measure(batch.size) {
            val it = batch.iterator
            it.map {
              case (_, row) =>
                queryContext.calculator.evaluatePostMap(rowBuilder, schema.tokenizer, row)
            }
          }
        }
      } else {
        mr.map(keysAndValuesWinFunc)(_._2)
      }

      mr.map(reduced) { row =>
        queryContext.calculator.evaluatePostAggregateExprs(rowBuilder, schema.tokenizer, row)
      }
    } else {

      val agg = mr.aggregate[InternalRow, InternalRow](values)(
        r => queryContext.calculator.evaluateZero(rowBuilder, schema.tokenizer, r),
        (a, r) => queryContext.calculator.evaluateSequence(rowBuilder, schema.tokenizer, a, r),
        (a, b) => queryContext.calculator.evaluateCombine(rowBuilder, schema.tokenizer, a, b)
      )

      val reduced =
        mr.map(agg)(row => queryContext.calculator.evaluatePostMap(rowBuilder, schema.tokenizer, row))

      mr.map(reduced) { row =>
        queryContext.calculator.evaluatePostAggregateExprs(rowBuilder, schema.tokenizer, row)
      }
    }

    val postFiltered = queryContext.query.postFilter match {
      case Some(_) =>
        mr.batchFlatMap(calculated, extractBatchSize) { batch =>
          metricCollector.postFilter.measure(batch.size) {
            val it = batch.iterator
            it.filter(row => queryContext.calculator.evaluatePostFilter(rowBuilder, schema.tokenizer, row))
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
              row
            }
          }
        }
      }

    finalizeQuery(queryContext, rowBuilder, result, metricCollector)
  }

  def readExternalLinks(
      queryContext: QueryContext,
      rowBuilder: InternalRowBuilder,
      rows: Seq[InternalRow]
  ): Seq[InternalRow] = {
    queryContext.linkExprs.groupBy(_.link).foldLeft(rows) {
      case (rows, (c, exprs)) =>
        val externalLink = linkService(c)
        externalLink.setLinkedValues(rowBuilder, rows, exprs.toSet)
    }
  }

  def substituteLinks(
      flatAndConditions: Seq[FlatAndCondition],
      metricCollector: MetricQueryCollector
  ): Seq[FlatAndCondition] = {

    flatAndConditions.map { tbc =>

      val linkServices = tbc.conditions
        .flatMap(c =>
          c.flatten.collect {
            case LinkExpr(link, _) => linkService(link)
          }
        )
        .distinct

      val transformations = linkServices.flatMap(service =>
        metricCollector.dynamicMetric(s"create_queries.link.${service.externalLink.linkName}").measure(1) {
          service.transformCondition(tbc)
        }
      )

      ConditionUtils.transform(tbc, transformations)
    }
  }

  def mergeCondition(facs: Seq[FlatAndCondition]): Option[Condition] = {
    val merged = FlatAndCondition.mergeByTime(facs)

    if (merged.size == 1) merged.head._3
    else if (merged.size > 1) {
      val ands = merged.map {
        case (f, t, c) =>
          AndExpr(Seq(GeExpr(TimeExpr, ConstantExpr(Time(f))), LtExpr(TimeExpr, ConstantExpr(Time(t)))) ++ c)
      }
      Some(OrExpr(ands))
    } else None
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
