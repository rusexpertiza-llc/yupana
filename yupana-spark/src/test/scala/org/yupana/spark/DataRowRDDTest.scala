package org.yupana.spark

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.joda.time.LocalDateTime
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.types.DataTypeMeta
import org.yupana.core.QueryContext
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.model.InternalRowBuilder

class DataRowRDDTest extends AnyFlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  lazy val spark: SparkSession = SparkSession.builder().master("local").appName("yupana test").getOrCreate()

  "DataRowRDD" should "convert itself to DataFrame" in {
    val query = Query(
      table = Tables.itemsKkmTable,
      fields = Seq(
        time.toField,
        dimension(Dimensions.ITEM).toField,
        metric(ItemTableMetrics.quantityField).toField,
        metric(ItemTableMetrics.sumField).toField
      ),
      from = const(Time(LocalDateTime.now())),
      to = const(Time(LocalDateTime.now().minusHours(2)))
    )
    val queryContext = QueryContext(query, None)

    val builder = new InternalRowBuilder(queryContext)

    val theTime = LocalDateTime.now().minusHours(1)

    val rdd = spark.sparkContext.parallelize(
      Seq(
        builder
          .set(Time(theTime))
          .set(dimension(Dimensions.ITEM), "болт М6")
          .set(metric(ItemTableMetrics.quantityField), 42d)
          .buildAndReset()
          .data
      )
    )

    val drRdd = new DataRowRDD(rdd, queryContext)
    val df = drRdd.toDF(spark)

    df.schema shouldEqual StructType(
      List(
        StructField("time", TimestampType),
        StructField("item", StringType),
        StructField("quantity", DoubleType),
        StructField("sum", DataTypes.createDecimalType(DecimalType.MAX_PRECISION, DataTypeMeta.MONEY_SCALE))
      )
    )

    df.count() shouldEqual 1
    val row = df.head()
    row.getTimestamp(0) shouldEqual new Timestamp(theTime.toDateTime.getMillis)
    row.getString(1) shouldEqual "болт М6"
    row.getDouble(2) shouldEqual 42d
    row.getDecimal(3) shouldEqual null
  }

}
