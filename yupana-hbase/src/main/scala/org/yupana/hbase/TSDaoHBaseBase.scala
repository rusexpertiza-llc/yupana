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

package org.yupana.hbase

import java.nio.ByteBuffer
import java.util

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, Dimension, RawDimension, Table }
import org.yupana.api.utils.{ PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.MapReducible
import org.yupana.core.dao._
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder }
import org.yupana.core.utils.TimeBoundedCondition
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.language.higherKinds

trait TSDaoHBaseBase[Collection[_]] extends TSReadingDao[Collection, Long] with StrictLogging {
  type IdType = Long
  type TimeFilter = Long => Boolean
  type RowFilter = TSDOutputRow => Boolean

  import org.yupana.core.utils.ConditionMatchers.{ Equ, Neq }

  val TIME: RawDimension[Time] = RawDimension[Time]("time")

  val CROSS_JOIN_LIMIT = 500000
  val RANGE_FILTERS_LIMIT = 100000
  val FUZZY_FILTERS_LIMIT = 20
  val EXTRACT_BATCH_SIZE = 10000

  def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Collection]
  def dictionaryProvider: DictionaryProvider

  def executeScans(
      queryContext: InternalQueryContext,
      from: Long,
      to: Long,
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): Collection[TSDOutputRow]

  override def query(
      query: InternalQuery,
      valueDataBuilder: InternalRowBuilder,
      metricCollector: MetricQueryCollector
  ): Collection[InternalRow] = {

    val tbc = TimeBoundedCondition(query.condition)

    if (tbc.size != 1) throw new IllegalArgumentException("Only one condition is supported")

    val condition = tbc.head

    val from = condition.from.getOrElse(throw new IllegalArgumentException("FROM time is not defined"))
    val to = condition.to.getOrElse(throw new IllegalArgumentException("TO time is not defined"))

    val filters = metricCollector.createDimensionFilters.measure(1) {
      val c = if (condition.conditions.nonEmpty) Some(AndExpr(condition.conditions)) else None
      createFilters(c)
    }

    val dimFilter = filters.allIncludes

    val prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]] = dimFilter.map {
      case (d, it) => d -> it.prefetch(RANGE_FILTERS_LIMIT)
    }

    val sizeLimitedRangeScanDims = rangeScanDimensions(query, prefetchedDimIterators)

    val rangeScanDimIds = if (dimFilter.exists(_._2.isEmpty)) {
      Iterator.empty
    } else {
      val rangeScanDimIterators = sizeLimitedRangeScanDims.map(d => d -> prefetchedDimIterators(d)).toMap
      rangeScanFilters(rangeScanDimIterators)
    }

    val context = InternalQueryContext(query, metricCollector)

    val rows = executeScans(context, from, to, rangeScanDimIds)

    val includeRowFilter = prefetchedDimIterators.filterKeys(d => !sizeLimitedRangeScanDims.contains(d))

    val rowFilter = createRowFilter(query.table, includeRowFilter, filters.allExcludes)
    val timeFilter = createTimeFilter(
      from,
      to,
      filters.excludeTime.map(_.toSet).getOrElse(Set.empty),
      filters.excludeTime.map(_.toSet).getOrElse(Set.empty)
    )

    val mr = mapReduceEngine(metricCollector)

    mr.batchFlatMap(rows, EXTRACT_BATCH_SIZE) { rs =>
      val filtered = context.metricsCollector.filterRows.measure(rs.size) {
        rs.filter(rowFilter)
      }
      extractData(context, valueDataBuilder, filtered, timeFilter).iterator
    }
  }

  def idsToValues(
      dimension: DictionaryDimension,
      ids: Set[IdType],
      metricCollector: MetricQueryCollector
  ): Map[IdType, String] = {
    dictionaryProvider.dictionary(dimension).values(ids, metricCollector)
  }

  def valuesToIds(dimension: DictionaryDimension, values: SortedSetIterator[String]): SortedSetIterator[IdType] = {
    val dictionary = dictionaryProvider.dictionary(dimension)
    val it = dictionary.findIdsByValues(values.toSet).values.toSeq.sortWith(dimension.rOrdering.lt).iterator
    SortedSetIterator(it)
  }

  private def rangeScanDimensions(
      query: InternalQuery,
      prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]]
  ) = {

    val continuousDims = query.table.dimensionSeq.takeWhile(prefetchedDimIterators.contains)
    val sizes = continuousDims
      .scanLeft(1L) {
        case (size, dim) =>
          val it = prefetchedDimIterators(dim)
          val itSize = if (it.isAllFetched) it.fetched.length else RANGE_FILTERS_LIMIT
          size * itSize
      }
      .drop(1)

    val sizeLimitedRangeScanDims = continuousDims.zip(sizes).takeWhile(_._2 <= CROSS_JOIN_LIMIT).map(_._1)
    sizeLimitedRangeScanDims
  }

  private def rangeScanFilters(
      dimensionIds: Map[Dimension, PrefetchedSortedSetIterator[_]]
  ): Iterator[Map[Dimension, Seq[_]]] = {

    val (completelyFetchedDimIts, partiallyFetchedDimIts) = dimensionIds.partition(_._2.isAllFetched)

    if (partiallyFetchedDimIts.size > 1) {
      throw new IllegalStateException(
        s"More then one dimension in query have size greater " +
          s"than $RANGE_FILTERS_LIMIT [${partiallyFetchedDimIts.keys.mkString(", ")}]"
      )
    }

    val fetchedDimIds = completelyFetchedDimIts.map { case (dim, ids) => dim -> ids.fetched.toSeq }

    partiallyFetchedDimIts.headOption match {
      case Some((pd, pids)) =>
        pids.grouped(RANGE_FILTERS_LIMIT).map { batch =>
          fetchedDimIds + (pd -> batch)
        }

      case None =>
        Iterator(fetchedDimIds)
    }
  }

  private def createTimeFilter(
      fromTime: Long,
      toTime: Long,
      includeSet: Set[Time],
      excludeSet: Set[Time]
  ): TimeFilter = {
    val baseFilter: TimeFilter = t => t >= fromTime && t < toTime
    val incMillis = includeSet.map(_.millis)
    val excMillis = excludeSet.map(_.millis)

    if (excMillis.nonEmpty) {
      if (incMillis.nonEmpty) { t =>
        baseFilter(t) && incMillis.contains(t) && !excMillis.contains(t)
      } else { t =>
        baseFilter(t) && !excMillis.contains(t)
      }
    } else {
      if (incMillis.nonEmpty) { t =>
        baseFilter(t) && incMillis.contains(t)
      } else {
        baseFilter
      }
    }
  }

  private def createRowFilter(
      table: Table,
      include: Map[Dimension, SortedSetIterator[_]],
      exclude: Map[Dimension, SortedSetIterator[_]]
  ): RowFilter = {

    val includeMap = include.map { case (k, v) => k -> v.toSet }
    val excludeMap = exclude.map { case (k, v) => k -> v.toSet }

    if (excludeMap.nonEmpty) {
      if (includeMap.nonEmpty) {
        rowFilter(
          table,
          (dim, x) => includeMap.get(dim).forall(_.contains(x)) && !excludeMap.get(dim).exists(_.contains(x))
        )
      } else {
        rowFilter(table, (dim, x) => !excludeMap.get(dim).exists(_.contains(x)))
      }
    } else {
      if (includeMap.nonEmpty) {
        rowFilter(table, (dim, x) => includeMap.get(dim).forall(_.contains(x)))
      } else { _ =>
        true
      }
    }
  }

  private def rowFilter(table: Table, f: (Dimension, Any) => Boolean): RowFilter = { row =>
    row.key.dimReprs.zip(table.dimensionSeq).forall {
      case (Some(x), dim) => f(dim, x)
      case _              => true
    }
  }

  def createFilters(condition: Option[Condition]): Filters = {
    def createFilters(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case Equ(DimensionExpr(dim), ConstantExpr(c: Any)) =>
          builder.includeValue(dim, c)

        case Equ(ConstantExpr(c: String), DimensionExpr(dim)) =>
          builder.includeValue(dim, c)

        case Equ(TimeExpr, ConstantExpr(c: Time)) =>
          builder.includeTime(c)

        case Equ(ConstantExpr(c: Time), TimeExpr) =>
          builder.includeTime(c)

        case InExpr(DimensionExpr(dim), consts) =>
          builder.includeValues(dim, consts)

        case InExpr(_: TimeExpr.type, consts) =>
          builder.includeTime(consts.asInstanceOf[Set[Time]])

        case DimIdInExpr(dim, dimIds) =>
          builder.includeIds(dim, dimIds)

        case Neq(DimensionExpr(dim), ConstantExpr(c: Any)) =>
          builder.excludeValue(dim, c)

        case Neq(ConstantExpr(c: String), DimensionExpr(dim)) =>
          builder.excludeValue(dim, c)

        case Neq(TimeExpr, ConstantExpr(c: Time)) =>
          builder.excludeTime(c)

        case Neq(ConstantExpr(c: Time), TimeExpr) =>
          builder.excludeTime(c)

        case NotInExpr(DimensionExpr(dim), consts) =>
          builder.excludeValues(dim, consts)

        case NotInExpr(_: TimeExpr.type, consts) =>
          builder.excludeTime(consts.asInstanceOf[Set[Time]])

        case DimIdNotInExpr(dim, dimIds) =>
          builder.excludeIds(dim, dimIds)

        case AndExpr(conditions) =>
          conditions.foldLeft(builder)((f, c) => createFilters(c, f))

        case InExpr(t: TupleExpr[_, _], vs) =>
          val filters1 = createFilters(InExpr(t.e1, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._1)), builder)
          createFilters(InExpr(t.e2, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._2)), filters1)

        case Equ(TupleExpr(e1, e2), ConstantExpr(v: (_, _))) =>
          val filters1 = createFilters(InExpr(e1.aux, Set(v._1.asInstanceOf[e1.Out])), builder)
          createFilters(InExpr(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case Equ(ConstantExpr(v: (_, _)), TupleExpr(e1, e2)) =>
          val filters1 = createFilters(InExpr(e1.aux, Set(v._1.asInstanceOf[e1.Out])), builder)
          createFilters(InExpr(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case _ => builder
      }
    }

    condition match {
      case Some(c) =>
        createFilters(c, Filters.newBuilder).build(valuesToIds)

      case None =>
        Filters.empty
    }
  }

  private def readRow(
      context: InternalQueryContext,
      bytes: Array[Byte],
      data: Array[Option[Any]],
      time: Long
  ): Boolean = {
    val bb = ByteBuffer.wrap(bytes)
    util.Arrays.fill(data.asInstanceOf[Array[AnyRef]], None)
    var correct = true
    while (bb.hasRemaining && correct) {
      val tag = bb.get()
      context.fieldIndexMap.get(tag) match {
        case Some(field) =>
          data(tag) = Some(field.dataType.storable.read(bb))

        case None =>
          correct = false
          logger.warn(s"Unknown tag: $tag, in table: ${context.table.name}, row time: $time")
      }
    }
    correct
  }

  private def extractData(
      context: InternalQueryContext,
      valueDataBuilder: InternalRowBuilder,
      rows: Seq[TSDOutputRow],
      timeFilter: TimeFilter
  ): Seq[InternalRow] = {

    val rowsWithDimIds = context.metricsCollector.extractDataComputation.measure(rows.size) {
      val maxTag = context.table.metrics.map(_.tag).max
      val rowValues = Array.ofDim[Option[Any]](maxTag + 1)
      for {
        row <- rows
        (offset, bytes) <- row.values.toSeq
        time = row.key.baseTime + offset
        if timeFilter(time) && readRow(context, bytes, rowValues, time)
      } yield {
        context.exprs.foreach {
          case e @ DimensionExpr(dim) =>
            valueDataBuilder.set(e, row.key.dimReprs(context.dimIndexMap(dim)))
          case e @ MetricExpr(field) =>
            valueDataBuilder.set(e, rowValues(field.tag))
          case TimeExpr => valueDataBuilder.set(TimeExpr, Some(Time(time)))
          case e =>
            throw new IllegalArgumentException(
              s"Unsupported expression $e passed to DAO"
            )
        }

        valueDataBuilder.buildAndReset()
      }
    }

    context.metricsCollector.dimensionValuesForIds.measure(rows.size) {
      context.exprs.foldLeft(rowsWithDimIds) { (accRows, expr) =>
        expr match {
          case e @ DimensionExpr(d: DictionaryDimension) =>
            val dimIdx = context.dimIndexMap(d)
            val dimIds = rows.flatMap(_.key.dimReprs(dimIdx)).toSet
            val dimValues = idsToValues(d, dimIds, context.metricsCollector)
            accRows.map { row =>
              val dimVal = row.get[Long](valueDataBuilder.exprIndex, e).flatMap(id => dimValues.get(id))
              row.set(valueDataBuilder.exprIndex, e, dimVal)
            }

          case _ => accRows
        }
      }
    }
  }

  override def isSupportedCondition(condition: Condition): Boolean = {
    condition match {
      case BinaryOperationExpr(_, _: TimeExpr.type, ConstantExpr(_)) => true
      case BinaryOperationExpr(_, ConstantExpr(_), _: TimeExpr.type) => true
      case _: DimIdInExpr                                            => true
      case _: DimIdNotInExpr                                         => true
      case Equ(_: DimensionExpr[_], ConstantExpr(_))                 => true
      case Equ(ConstantExpr(_), _: DimensionExpr[_])                 => true
      case Neq(_: DimensionExpr[_], ConstantExpr(_))                 => true
      case Neq(ConstantExpr(_), _: DimensionExpr[_])                 => true
      case InExpr(_: DimensionExpr[_], _)                            => true
      case NotInExpr(_: DimensionExpr[_], _)                         => true
      case _                                                         => false
    }
  }
}
