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

package org.yupana.spark

import java.sql.{ Timestamp, Types }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types._
import org.apache.spark.{ Partition, TaskContext }
import org.joda.time.DateTimeZone
import org.yupana.api.Time
import org.yupana.api.query.{ DataRow, QueryField }
import org.yupana.core.{ QueryContext, TsdbServerResultBase }

class DataRowRDD(val underlying: RDD[Array[Option[Any]]], override val queryContext: QueryContext)
    extends RDD[DataRow](underlying)
    with TsdbServerResultBase {

  override def compute(split: Partition, context: TaskContext): Iterator[DataRow] = {
    underlying
      .iterator(split, context)
      .map(data => new DataRow(data, dataIndexForFieldName, dataIndexForFieldIndex))
  }

  override protected def getPartitions: Array[Partition] = underlying.partitions

  def toDF(spark: SparkSession): DataFrame = {
    val rowRdd = underlying.map(createRow)
    spark.createDataFrame(rowRdd, createSchema)
  }

  private def createSchema: StructType = {
    val fields = queryContext.query.fields.map(fieldToSpark)
    StructType(fields)
  }

  private def createRow(a: Array[Option[Any]]): Row = {
    val values = queryContext.query.fields.indices.map(
      idx =>
        a(dataIndexForFieldIndex(idx)) match {
          case Some(Time(t)) => new Timestamp(DateTimeZone.getDefault.convertLocalToUTC(t, false))
          case x             => x.orNull
        }
    )
    Row(values: _*)
  }

  private def fieldToSpark(field: QueryField): StructField = {
    val dataType = field.expr.dataType
    val sparkType = DataRowRDD.TYPE_MAP
      .getOrElse(dataType.meta.sqlType, throw new IllegalArgumentException(s"Unsupported data type $dataType"))

    StructField(field.name, sparkType, nullable = true)
  }
}

object DataRowRDD {
  private[DataRowRDD] val TYPE_MAP: Map[Int, DataType] = Map(
    Types.BOOLEAN -> BooleanType,
    Types.VARCHAR -> StringType,
    Types.INTEGER -> IntegerType,
    Types.DOUBLE -> DoubleType,
    Types.BIGINT -> LongType,
    Types.DECIMAL -> DataTypes.createDecimalType(DecimalType.MAX_PRECISION, 2),
    Types.TIMESTAMP -> TimestampType
  )
}
