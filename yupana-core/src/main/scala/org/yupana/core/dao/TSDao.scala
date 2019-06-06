package org.yupana.core.dao

import org.yupana.api.query.DataPoint
import org.yupana.api.schema.Table

import scala.language.higherKinds

trait TSDao[Collection[_], IdType] extends TSReadingDao[Collection, IdType] {
  def put(dataPoints: Seq[DataPoint]): Unit

  def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)]
  def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit
  def checkAndPutRollupStatus(time: Long, oldStatus: Option[String], newStatus: String, table: Table): Boolean

  def getRollupSpecialField(fieldName: String, table: Table): Option[Long]
  def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit
}
