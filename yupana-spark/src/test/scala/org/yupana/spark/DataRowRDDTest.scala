package org.yupana.spark

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types._
import org.joda.time.LocalDateTime
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core.QueryContext
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

class DataRowRDDTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  import org.yupana.api.query.syntax.All._

  "DataRowRDD" should "convert itself to DataFrame" in {
    val query = Query(
      table = Tables.itemsKkmTable,
      fields = Seq(
        time.toField,
        dimension(Dimensions.ITEM_TAG).toField,
        metric(ItemTableMetrics.quantityField).toField,
        metric(ItemTableMetrics.sumField).toField
      ),
      from = const(Time(LocalDateTime.now())),
      to = const(Time(LocalDateTime.now().minusHours(2)))
    )
    val queryContext = QueryContext(query, None)

    val theTime = LocalDateTime.now().minusHours(1)

    val rdd = sc.parallelize(
      Seq(
        Array(
          Some(Time(theTime)),
          Some("болт М6"),
          Some(42d),
          None
        )
      )
    )

    val drRdd = new DataRowRDD(rdd, queryContext)
    val df = drRdd.toDF(spark)

    df.schema shouldEqual StructType(
      List(
        StructField("time", TimestampType),
        StructField("item", StringType),
        StructField("quantity", DoubleType),
        StructField("sum", DataTypes.createDecimalType(DecimalType.MAX_PRECISION, 2))
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
