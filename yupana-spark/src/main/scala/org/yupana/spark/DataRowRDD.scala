package org.yupana.spark

import java.sql.{Timestamp, Types}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import org.joda.time.DateTimeZone
import org.yupana.api.Time
import org.yupana.api.query.{DataRow, QueryField}
import org.yupana.core.{QueryContext, TsdbServerResultBase}

class DataRowRDD(override val rows: RDD[Array[Option[Any]]], @transient override val queryContext: QueryContext)
  extends RDD[DataRow](rows)
  with TsdbServerResultBase[RDD] {

  override def compute(split: Partition, context: TaskContext): Iterator[DataRow] = {
    rows
      .iterator(split, context)
      .map(data => new DataRow(data, dataIndexForFieldName, dataIndexForFieldIndex))
  }

  override protected def getPartitions: Array[Partition] = rows.partitions

  def toDF(spark: SparkSession): DataFrame = {
    val fields = queryContext.query.fields
    val rowRdd = rows.map(v => createRow(v , fields))
    spark.createDataFrame(rowRdd, createSchema)
  }

  private def createSchema: StructType = {
    val fields = queryContext.query.fields.map(fieldToSpark)
    StructType(fields)
  }

  private def createRow(a: Array[Option[Any]], fields: Seq[QueryField]): Row = {
    val values = fields.indices.map(idx =>
      a(dataIndexForFieldIndex(idx)) match {
        case Some(Time(t)) => new Timestamp(DateTimeZone.getDefault.convertLocalToUTC(t, false))
        case x => x.orNull
      }
    )
    Row(values:_*)
  }

  private def fieldToSpark(field: QueryField): StructField = {
    val dataType = field.expr.dataType
    val sparkType = DataRowRDD.TYPE_MAP.getOrElse(dataType.meta.sqlType, throw new IllegalArgumentException(s"Unsupported data type $dataType"))

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
