package org.yupana.core.sql

import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UpdatesIntervalsProviderTest extends AnyFlatSpec with Matchers {

  import org.yupana.core.providers.UpdatesIntervalsProvider._
  import org.yupana.core.sql.parser._

  "UpdatesIntervalsProvider" should "create filters" in {
    createFilter(None) shouldBe UpdatesIntervalsFilter.empty

    createFilter(
      Some(Eq(FieldName("table"), Constant(StringValue("some_table"))))
    ) shouldBe UpdatesIntervalsFilter.empty
      .withTableName("some_table")
    createFilter(
      Some(
        BetweenCondition(
          FieldName("updated_at"),
          TimestampValue(DateTime.parse("2021-06-01")),
          TimestampValue(DateTime.parse("2021-06-02"))
        )
      )
    ) shouldBe UpdatesIntervalsFilter.empty
      .withFrom(DateTime.parse("2021-06-01"))
      .withTo(DateTime.parse("2021-06-02"))

    createFilter(
      Some(Eq(FieldName("updated_by"), Constant(StringValue("somebody"))))
    ) shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(StringValue("some_table"))),
            BetweenCondition(
              FieldName("updated_at"),
              TimestampValue(DateTime.parse("2021-06-01")),
              TimestampValue(DateTime.parse("2021-06-02"))
            ),
            Eq(FieldName("updated_by"), Constant(StringValue("somebody")))
          )
        )
      )
    ) shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withTo(DateTime.parse("2021-06-02"))
      .withFrom(DateTime.parse("2021-06-01"))
      .withTableName("some_table")

    createFilter(
      Some(Eq(FieldName("unknown_field"), Constant(StringValue("unknown"))))
    ) shouldBe UpdatesIntervalsFilter.empty
  }
}
