package org.yupana.spark

import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.types.DataTypeMeta
import org.yupana.core.QueryContext
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.yupana.core.jit.JIT
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.utils.RussianTokenizer

import java.time.{ LocalDateTime, ZoneOffset }

trait DataRowRDDTest extends AnyFlatSpecLike with Matchers with SharedSparkSession {

  import org.yupana.api.query.syntax.All._

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
    val queryContext = new QueryContext(query, None, RussianTokenizer, JIT, NoMetricCollector)

    val theTime = LocalDateTime.now().minusHours(1)

    val rdd = spark.sparkContext.parallelize {

      val batch = BatchDataset(queryContext)
      batch.set(0, Time(theTime))
      batch.set(0, dimension(Dimensions.ITEM), "болт М6")
      batch.set(0, metric(ItemTableMetrics.quantityField), 42d)
      Seq(batch)

    }

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
    row.getTimestamp(0) shouldEqual new Timestamp(theTime.toInstant(ZoneOffset.UTC).toEpochMilli)
    row.getString(1) shouldEqual "болт М6"
    row.getDouble(2) shouldEqual 42d
    row.getDecimal(3) shouldEqual null
  }

}
