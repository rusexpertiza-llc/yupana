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

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.codec.binary.Hex
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, Dimension, RawDimension, Table }
import org.yupana.api.utils.{ PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.api.Time
import org.yupana.core.MapReducible
import org.yupana.core.dao._
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.TimeBoundedCondition
import org.apache.hadoop.hbase.client.{ Result => HResult }
import org.yupana.core.utils.ConditionMatchers._

import scala.language.higherKinds
import scala.util.Try

trait TSDaoHBaseBase[Collection[_]] extends TSReadingDao[Collection, Long] with StrictLogging {
  type IdType = Long
  type TimeFilter = Long => Boolean
  type RowFilter = TSDRowKey => Boolean

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
  ): Collection[HResult]

  override def query(
      query: InternalQuery,
      internalRowBuilder: InternalRowBuilder,
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
    val hasEmptyFilter = dimFilter.exists(_._2.isEmpty)

    val prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]] = dimFilter.map {
      case (d, it) =>
        val rit = it.asInstanceOf[SortedSetIterator[d.R]]
        d -> rit.prefetch(RANGE_FILTERS_LIMIT)(d.rCt)
    }.toMap

    val sizeLimitedRangeScanDims = rangeScanDimensions(query, prefetchedDimIterators)

    val rangeScanDimIds = if (hasEmptyFilter) {
      Iterator.empty
    } else {
      val rangeScanDimIterators = sizeLimitedRangeScanDims.map { d =>
        (d -> prefetchedDimIterators(d)).asInstanceOf[(Dimension, PrefetchedSortedSetIterator[_])]
      }.toMap
      rangeScanFilters(rangeScanDimIterators)
    }

    val context = InternalQueryContext(query, metricCollector)

    val rows = executeScans(context, from, to, rangeScanDimIds)

    val includeRowFilter = prefetchedDimIterators.filterKeys(d => !sizeLimitedRangeScanDims.contains(d))

    val excludeRowFilter = filters.allExcludes.filterKeys(d => !sizeLimitedRangeScanDims.contains(d))

    val rowFilter = createRowFilter(query.table, includeRowFilter, excludeRowFilter)
    val timeFilter = createTimeFilter(
      from,
      to,
      filters.includeTime.map(_.toSet).getOrElse(Set.empty),
      filters.excludeTime.map(_.toSet).getOrElse(Set.empty)
    )

    val mr = mapReduceEngine(metricCollector)

    val table = query.table
    mr.batchFlatMap(rows, EXTRACT_BATCH_SIZE) { rs =>
      val filtered = context.metricsCollector.filterRows.measure(rs.size) {
        rs.filter(r => rowFilter(HBaseUtils.parseRowKey(r.getRow, table)))
      }

      new TSDHBaseRowIterator(context, filtered.iterator, internalRowBuilder)
        .filter(r => timeFilter(r.get[Time](internalRowBuilder.timeIndex).millis))
    }
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

  private def rowFilter(table: Table, f: (Dimension, Any) => Boolean): RowFilter = { rowKey =>
    rowKey.dimReprs.zip(table.dimensionSeq).forall {
      case (Some(x), dim) => f(dim, x)
      case _              => true
    }
  }

  private def dimIdValueFromString[R](dim: Dimension.Aux2[_, R], value: String): Option[R] = {
    Try(Hex.decodeHex(value.toCharArray)).toOption.map(dim.rStorable.read)
  }

  def createFilters(condition: Option[Condition]): Filters = {
    def handleEq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case EqExpr(DimensionExpr(dim), ConstantExpr(c)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqExpr(ConstantExpr(c), DimensionExpr(dim)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqString(ConstantExpr(c), LowerExpr(DimensionExpr(dim))) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqExpr(DimensionIdExpr(dim), ConstantExpr(c: String)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqExpr(ConstantExpr(c: String), DimensionIdExpr(dim)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqExpr(_: TimeExpr.type, ConstantExpr(c: Time)) =>
          builder.includeTime(c)

        case EqExpr(ConstantExpr(c: Time), _: TimeExpr.type) =>
          builder.includeTime(c)

        case EqUntyped(TupleExpr(e1, e2), ConstantExpr(v: (_, _))) =>
          val filters1 = createFilters(InExpr(e1.aux, Set(v._1.asInstanceOf[e1.Out])), builder)
          createFilters(InExpr(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case EqUntyped(ConstantExpr(v: (_, _)), t: TupleExpr[_, _]) =>
          val filters1 = createFilters(InExpr(t.e1.aux, Set(v._1.asInstanceOf[t.e1.Out])), builder)
          createFilters(InExpr(t.e2.aux, Set(v._2.asInstanceOf[t.e2.Out])), filters1)

        case _ => builder
      }
    }

    def handleNeq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case NeqExpr(DimensionExpr(dim), ConstantExpr(c)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqExpr(ConstantExpr(c), DimensionExpr(dim)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqString(ConstantExpr(c), LowerExpr(DimensionExpr(dim))) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqExpr(DimensionIdExpr(dim), ConstantExpr(c: String)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqExpr(ConstantExpr(c: String), DimensionIdExpr(dim)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqExpr(_: TimeExpr.type, ConstantExpr(c: Time)) =>
          builder.excludeTime(c)

        case NeqExpr(ConstantExpr(c: Time), _: TimeExpr.type) =>
          builder.excludeTime(c)

        case _ => builder
      }
    }

    def createFilters(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case EqExpr(_, _) => handleEq(condition, builder)

        case NeqExpr(_, _) => handleNeq(condition, builder)

        case InExpr(DimensionExpr(dim), consts) =>
          builder.includeValues(dim, consts)

        case InString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.includeValues(
            dim,
            consts.asInstanceOf[Set[dim.T]]
          )

        case InExpr(_: TimeExpr.type, consts) =>
          builder.includeTime(consts.asInstanceOf[Set[Time]])

        case DimIdInExpr(dim, dimIds) =>
          builder.includeIds(dim, dimIds)

        case InExpr(DimensionIdExpr(dim), dimIds) =>
          builder.includeIds(
            dim.aux,
            dimIds.asInstanceOf[Set[String]].toSeq.flatMap(v => dimIdValueFromString(dim.aux, v))
          )

        case NotInExpr(DimensionExpr(dim), consts) =>
          builder.excludeValues(dim, consts.asInstanceOf[Set[dim.T]])

        case NotInString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.excludeValues(dim, consts.asInstanceOf[Set[dim.T]])

        case NotInExpr(DimensionIdExpr(dim), dimIds) =>
          builder.excludeIds(
            dim.aux,
            dimIds.asInstanceOf[Set[String]].toSeq.flatMap(v => dimIdValueFromString(dim.aux, v))
          )

        case NotInExpr(_: TimeExpr.type, consts) =>
          builder.excludeTime(consts.asInstanceOf[Set[Time]])

        case DimIdNotInExpr(dim, dimIds) =>
          builder.excludeIds(dim, dimIds)

        case AndExpr(conditions) =>
          conditions.foldLeft(builder)((f, c) => createFilters(c, f))

        case InUntyped(t: TupleExpr[_, _], vs) =>
          val filters1 = createFilters(InExpr(t.e1, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._1)), builder)
          createFilters(InExpr(t.e2, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._2)), filters1)

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

  override def isSupportedCondition(condition: Condition): Boolean = {
    condition match {
      case EqExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
      case EqExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
      case NeqExpr(_: TimeExpr.type, ConstantExpr(_))                 => true
      case NeqExpr(ConstantExpr(_), _: TimeExpr.type)                 => true
      case GtExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
      case GtExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
      case LtExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
      case LtExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
      case GeExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
      case GeExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
      case LeExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
      case LeExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
      case _: DimIdInExpr[_, _]                                       => true
      case _: DimIdNotInExpr[_, _]                                    => true
      case EqExpr(_: DimensionExpr[_], ConstantExpr(_))               => true
      case EqExpr(ConstantExpr(_), _: DimensionExpr[_])               => true
      case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_))  => true
      case EqString(ConstantExpr(_), LowerExpr(_: DimensionExpr[_]))  => true
      case NeqExpr(_: DimensionExpr[_], ConstantExpr(_))              => true
      case NeqExpr(ConstantExpr(_), _: DimensionExpr[_])              => true
      case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_)) => true
      case NeqString(LowerExpr(ConstantExpr(_)), _: DimensionExpr[_]) => true
      case EqExpr(_: DimensionIdExpr, ConstantExpr(_))                => true
      case EqExpr(ConstantExpr(_), _: DimensionIdExpr)                => true
      case NeqExpr(_: DimensionIdExpr, ConstantExpr(_))               => true
      case NeqExpr(ConstantExpr(_), _: DimensionIdExpr)               => true
      case InExpr(_: DimensionExpr[_], _)                             => true
      case NotInExpr(_: DimensionExpr[_], _)                          => true
      case InString(LowerExpr(_: DimensionExpr[_]), _)                => true
      case NotInString(LowerExpr(_: DimensionExpr[_]), _)             => true
      case InExpr(_: DimensionIdExpr, _)                              => true
      case NotInExpr(_: DimensionIdExpr, _)                           => true
      case _                                                          => false
    }
  }
}
