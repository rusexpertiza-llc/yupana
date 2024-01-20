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
import org.yupana.api.{ Blob, Time }
import org.yupana.api.query.{ DataRow, QueryField }
import org.yupana.api.types.ArrayDataType
import org.yupana.api.types.DataType.TypeKind
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.{ QueryContext, TsdbResultBase }

class DataRowRDD(override val rows: RDD[InternalRow], override val queryContext: QueryContext)
    extends RDD[DataRow](rows)
    with TsdbResultBase[RDD] {

  private val fields = queryContext.query.fields.toArray

  override lazy val internalRowBuilder: InternalRowBuilder = new InternalRowBuilder(queryContext)

  override def compute(split: Partition, context: TaskContext): Iterator[DataRow] = {
    rows
      .iterator(split, context)
      .map(row => dataRow(row))
  }

  override protected def getPartitions: Array[Partition] = rows.partitions

  def toDF(spark: SparkSession): DataFrame = {
    val rowRdd = rows.map(v => createRow(v))
    spark.createDataFrame(rowRdd, createSchema)
  }

  private def createSchema: StructType = {
    val fields = queryContext.query.fields.map(fieldToSpark)
    StructType(fields)
  }

  private def createRow(internalRow: InternalRow): Row = {
    val values = fields.indices.map { i =>
      val index = dataIndexForFieldIndex(i)
      val field = fields(i)
      if (internalRow.isDefined(internalRowBuilder, index)) {
        internalRow.get(internalRowBuilder, index)(field.expr.dataType.internalStorable) match {
          case t @ Time(_) => new Timestamp(t.toDateTime.toInstant.toEpochMilli)
          case Blob(bytes) => bytes
          case x           => x
        }
      } else null
    }
    Row(values: _*)
  }

  private def fieldToSpark(field: QueryField): StructField = {
    val sparkType = DataRowRDD.yupanaToSparkType(field.expr.dataType)
    StructField(field.name, sparkType, nullable = true)
  }

}

object DataRowRDD {
  private[DataRowRDD] val TYPE_MAP: Map[Int, DataType] = Map(
    Types.BOOLEAN -> BooleanType,
    Types.VARCHAR -> StringType,
    Types.TINYINT -> ByteType,
    Types.SMALLINT -> ShortType,
    Types.INTEGER -> IntegerType,
    Types.DOUBLE -> DoubleType,
    Types.BIGINT -> LongType,
    Types.TIMESTAMP -> TimestampType,
    Types.BLOB -> ArrayType(ByteType)
  )

  def yupanaToSparkType(yupanaDataType: org.yupana.api.types.DataType): DataType = {
    if (yupanaDataType.meta.sqlType == Types.DECIMAL) {
      DataTypes.createDecimalType(DecimalType.MAX_PRECISION, yupanaDataType.meta.scale)
    } else if (yupanaDataType.kind == TypeKind.Array) {
      val adt = yupanaDataType.asInstanceOf[ArrayDataType[_]]
      val innerType = yupanaToSparkType(adt.valueType)
      ArrayType(innerType)
    } else {
      TYPE_MAP.getOrElse(
        yupanaDataType.meta.sqlType,
        throw new IllegalArgumentException(s"Unsupported data type $yupanaDataType")
      )
    }
  }
}
