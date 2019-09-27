package org.yupana.akka

import org.yupana.core.TSDB
import org.yupana.api.Time
import org.yupana.api.query.{Result, SimpleResult}
import org.yupana.api.types.DataType
import org.yupana.core.dao.QueryMetricsFilter
import org.yupana.core.model.QueryStates

object QueryInfoProvider {

  def handleShowQueries(tsdb: TSDB, queryId: Option[String], limit: Int = 20): Result = {
    import org.yupana.core.model.TsdbQueryMetrics._

    val queries = tsdb.metricsDao.queriesByFilter(QueryMetricsFilter(
      limit = limit,
      queryId = queryId
    ))
    val data: Iterator[Array[Option[Any]]] = queries.map { query =>
      Array[Option[Any]](
        Some(query.queryId),
        Some(query.state.name),
        Some(query.query),
        Some(Time(query.startDate)),
        Some(query.totalDuration)
      ) ++ qualifiers.flatMap { q =>
        val metric = query.metrics(q)
        Array(Some(metric.count.toDouble), Some(metric.time), Some(metric.speed))
      }
    }.iterator

    val queryFieldNames = List(queryIdColumn, stateColumn, queryColumn, startDateColumn, totalDurationColumn) ++
      qualifiers.flatMap(q => List(q + "_" + metricCount, q + "_" + metricTime, q + "_" + metricSpeed))

    val queryFieldTypes = List(DataType[String], DataType[String], DataType[String], DataType[Time], DataType[Double]) ++
      (0 until qualifiers.size * 3).map(_ => DataType[Double])

    SimpleResult(queryFieldNames, queryFieldTypes, data)
  }

  def handleKillQuery(tsdb: TSDB, id: String): Result = {
    val result = if (tsdb.metricsDao.setQueryState(id, QueryStates.Cancelled)) {
      "OK"
    } else {
      "QUERY NOT FOUND"
    }
    SimpleResult(List("RESULT"), List(DataType[String]), Iterator(Array(Some(result))))
  }
}
