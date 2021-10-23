package org.yupana.core.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ LocalDate, LocalTime, OffsetDateTime, ZoneOffset }

class UpdatesIntervalsProviderTest extends AnyFlatSpec with Matchers {

  import org.yupana.core.providers.UpdatesIntervalsProvider._
  import org.yupana.core.sql.parser._

  private val startTime = OffsetDateTime.of(LocalDate.of(2016, 6, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
  private val endTime = OffsetDateTime.of(LocalDate.of(2016, 6, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  "UpdatesIntervalsProvider" should "create filters" in {
    createFilter(None) shouldBe UpdatesIntervalsFilter.empty

    createFilter(Some(Eq(FieldName("table"), Constant(StringValue("some_table"))))) shouldBe UpdatesIntervalsFilter.empty
      .withTableName("some_table")
    createFilter(
      Some(
        BetweenCondition(
          FieldName("updated_at"),
          TimestampValue(startTime),
          TimestampValue(endTime)
        )
      )
    ) shouldBe UpdatesIntervalsFilter.empty
      .withFrom(startTime)
      .withTo(endTime)

    createFilter(Some(Eq(FieldName("updated_by"), Constant(StringValue("somebody"))))) shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(StringValue("some_table"))),
            BetweenCondition(
              FieldName("updated_at"),
              TimestampValue(startTime),
              TimestampValue(endTime)
            ),
            Eq(FieldName("updated_by"), Constant(StringValue("somebody")))
          )
        )
      )
    ) shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withTo(endTime)
      .withFrom(startTime)
      .withTableName("some_table")

    createFilter(Some(Eq(FieldName("unknown_field"), Constant(StringValue("unknown"))))) shouldBe UpdatesIntervalsFilter.empty
  }
}
