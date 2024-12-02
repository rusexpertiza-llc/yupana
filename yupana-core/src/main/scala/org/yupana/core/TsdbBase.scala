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
import org.yupana.api.schema.{ ExternalLink, Schema, Table }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.jit.ExpressionCalculatorFactory
import org.yupana.core.model.{ BatchDataset, HashTableDataset, InternalQuery, UpdateInterval }
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.core.utils.{ ConditionUtils, FlatAndCondition }
import org.yupana.metrics.Failed

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
      keysAndValues: Collection[BatchDataset]
  ): Collection[BatchDataset]

  def createMetricCollector(query: Query, user: YupanaUser): MetricQueryCollector

  def finalizeQuery(
      queryContext: QueryContext,
      rows: Collection[BatchDataset],
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
  def query(
      query: Query,
      startTime: Time = Time(System.currentTimeMillis()),
      user: YupanaUser = YupanaUser.ANONYMOUS
  ): Result = {

    val preparedQuery = prepareQuery(query)
    logger.info(s"User ${user.name} start TSDB query with ${preparedQuery.uuidLog} start: " + preparedQuery)

    val optimizedQuery = QueryOptimizer.optimize(constantCalculator)(preparedQuery)

    logger.debug(s"Optimized query: $optimizedQuery")

    val metricCollector = createMetricCollector(optimizedQuery, user)
    val mr = mapReduceEngine(metricCollector)

    metricCollector.start()

    val (rows, queryContext) = query.table match {
      case Some(table) =>
        optimizedQuery.filter match {
          case Some(FalseExpr) =>
            val qc =
              metricCollector.createContext.measure(1)(
                new QueryContext(
                  optimizedQuery,
                  startTime,
                  optimizedQuery.filter,
                  schema.tokenizer,
                  calculatorFactory,
                  metricCollector
                )
              )

            mr.empty[BatchDataset] -> qc

          case Some(conditionAsIs) =>
            val withPlaceholders = fillPlaceholders(conditionAsIs, startTime, query.params)

            val flatAndCondition = FlatAndCondition(constantCalculator, withPlaceholders)

            val substitutedCondition = substituteLinks(flatAndCondition, startTime, user, metricCollector)
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
                new QueryContext(
                  optimizedQuery,
                  startTime,
                  finalPostDaoCondition,
                  schema.tokenizer,
                  calculatorFactory,
                  metricCollector
                )
              )

            val daoExprs = qc.datasetSchema.exprIndex.keys.collect {
              case e: DimensionExpr[_] => e
              case e: DimensionIdExpr  => e
              case e: MetricExpr[_]    => e
              case TimeExpr            => TimeExpr
            }

            val internalQuery =
              new InternalQuery(table, daoExprs.toSet[Expression[_]], substitutedCondition, query.hints)

            val rows = dao.query(internalQuery, qc, qc.datasetSchema, metricCollector)

            (rows, qc)

          case None =>
            val th = new IllegalArgumentException("Empty condition")
            metricCollector.setQueryStatus(Failed(th))
            throw th
        }
      case None =>
        val qc =
          metricCollector.createContext.measure(1)(
            new QueryContext(
              optimizedQuery,
              startTime,
              query.filter,
              schema.tokenizer,
              calculatorFactory,
              metricCollector
            )
          )

        val ds = new BatchDataset(qc.datasetSchema)
        ds.removeDeleted(0)
        val rows = mr.singleton(ds)
        (rows, qc)
    }

    processRows(queryContext, metricCollector, mr, rows)
  }

  def processRows(
      queryContext: QueryContext,
      metricCollector: MetricQueryCollector,
      mr: MapReducible[Collection],
      rows: Collection[BatchDataset]
  ): Result = {

    val hasWindowFunctions = queryContext.query.fields.exists(_.expr.kind == Window)
    val hasAggregates =
      queryContext.query.fields.exists(_.expr.kind == Aggregate) || queryContext.query.groupBy.nonEmpty

    val stage1res = mr.map(rows) { batch =>

      metricCollector.readExternalLinks.measure(batch.size) {
        readExternalLinks(queryContext, batch)
      }

      metricCollector.filter.measure(batch.size) {
        queryContext.calculator.evaluateFilter(batch)
      }
      metricCollector.evaluateExpressions.measure(batch.size) {
        queryContext.calculator.evaluateExpressions(batch)
      }
      batch
    }

    val stage2res = if (hasAggregates && !hasWindowFunctions) {
      val aggregated = mr.aggregateDatasets(stage1res, queryContext)(
        (acc: HashTableDataset, batch: BatchDataset) => {
          metricCollector.reduceOperation.measure(batch.size) {
            queryContext.calculator.evaluateFold(acc, batch)
          }
        },
        (acc: HashTableDataset, batch: BatchDataset) => {
          metricCollector.reduceOperation.measure(batch.size) {
            queryContext.calculator.evaluateCombine(acc, batch)
          }
        }
      )

      mr.map(aggregated) { batch =>
        metricCollector.reduceOperation.measure(batch.size) {
          queryContext.calculator.evaluatePostCombine(batch)
        }
        batch
      }
    } else if (hasWindowFunctions) {
      metricCollector.windowFunctions.measure(1) {
        applyWindowFunctions(queryContext, stage1res)
      }
    } else {
      stage1res
    }

    val stage3res = mr.map(stage2res) { batch =>
      queryContext.calculator.evaluatePostAggregateExprs(batch)
      batch
    }

    val stage4res = if (queryContext.query.postFilter.isDefined) {
      mr.map(stage3res) { batch =>
        metricCollector.postFilter.measure(batch.size) {
          queryContext.calculator.evaluatePostFilter(batch)
        }
        batch
      }
    } else {
      stage3res
    }

    val stage5res = queryContext.query.limit match {
      case Some(n) => mr.limit(stage4res)(n)
      case None    => stage4res
    }

    finalizeQuery(queryContext, stage5res, metricCollector)
  }

  def fillPlaceholders(c: Condition, startTime: Time, params: IndexedSeq[Any]): Condition = {
    c.transform(new Expression.Transform {
      override def apply[T](x: Expression[T]): Option[Expression[T]] = {
        x match {
          case PlaceholderExpr(id, t) =>
            if (id < params.length + 1) Some(ConstantExpr(params(id - 1).asInstanceOf[T])(t))
            else throw new IllegalStateException(s"Parameter #$id value is not defined")

          case NowExpr => Some(ConstantExpr(startTime))
          case _       => None
        }
      }
    })
  }

  def readExternalLinks(
      queryContext: QueryContext,
      ds: BatchDataset
  ): Unit = {
    queryContext.linkExprs.groupBy(_.link).foreach {
      case (externalLink, exprs) =>
        val externalLinkService = linkService(externalLink)
        externalLinkService.setLinkedValues(ds, exprs.toSet)
    }
  }

  def substituteLinks(
      flatAndConditions: Seq[FlatAndCondition],
      startTime: Time,
      user: YupanaUser,
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
          service.transformCondition(tbc, startTime, user)
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

    updateIntervals(updatedIntervals)
  }

  def putDataset(table: Table, dataset: Collection[BatchDataset], user: YupanaUser = YupanaUser.ANONYMOUS): Unit = {
    val mr = mapReduceEngine(NoMetricCollector)

    val updated = mr.map(dataset) { batch =>
      externalLinkServices.foreach(s => s.put(batch))
      batch
    }

    val updatedIntervals = dao.putDataset(mr, table, updated, user.name)

    updateIntervals(updatedIntervals)

  }

  private def updateIntervals(updateIntervals: Collection[UpdateInterval]): Unit = {
    val mr = mapReduceEngine(NoMetricCollector)
    val updateIntervalsByWhatUpdated = mr.map(updateIntervals)(i => i.whatUpdated -> i)
    val mostRecentUpdateIntervalsByWhatUpdated =
      mr.reduceByKey(updateIntervalsByWhatUpdated)((i1, i2) => if (i1.updatedAt.isAfter(i2.updatedAt)) i1 else i2)
    val mostRecentUpdateIntervals = mr.map(mostRecentUpdateIntervalsByWhatUpdated)(_._2)
    val materializedIntervals = mr.materialize(mostRecentUpdateIntervals).distinct
    changelogDao.putUpdatesIntervals(materializedIntervals)
  }

}
