package org.yupana.core.sql

import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ LocalDate, LocalTime, OffsetDateTime, ZoneOffset }

class UpdatesIntervalsProviderTest extends AnyFlatSpec with Matchers with EitherValues {

  import org.yupana.core.providers.UpdatesIntervalsProvider._
  import org.yupana.core.sql.parser._

  private val startTime = OffsetDateTime.of(LocalDate.of(2016, 6, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
  private val endTime = OffsetDateTime.of(LocalDate.of(2016, 6, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  "UpdatesIntervalsProvider" should "create empty filter" in {
    createFilter(None).value shouldBe UpdatesIntervalsFilter.empty
  }

  it should "handle table name filter" in {
    createFilter(
      Some(Eq(FieldName("table"), Constant(StringValue("some_table"))))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withTableName("some_table")
  }

  it should "support between time filter" in {
    createFilter(
      Some(
        BetweenCondition(
          FieldName("updated_at"),
          TimestampValue(startTime),
          TimestampValue(endTime)
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withFrom(startTime)
      .withTo(endTime)
  }

  it should "support updater filter" in {
    createFilter(
      Some(Eq(FieldName("updated_by"), Constant(StringValue("somebody"))))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
  }

  it should "combine filters" in {

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
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withTo(endTime)
      .withFrom(startTime)
      .withTableName("some_table")
  }

  it should "ignore unknown fields" in {
    createFilter(
      Some(Eq(FieldName("unknown_field"), Constant(StringValue("unknown"))))
    ).left.value should startWith("Unsupported condition")
  }

  it should "handle placeholders" in {
    createFilter(
      Some(
        And(
          Seq(
            BetweenCondition(FieldName("updated_at"), Placeholder(1), Placeholder(2)),
            Eq(FieldName("table"), Constant(Placeholder(3)))
          )
        )
      ),
      Map(1 -> TimestampValue(startTime), 2 -> TimestampValue(endTime), 3 -> StringValue("the_table"))
    ).value shouldBe UpdatesIntervalsFilter.empty.withFrom(startTime).withTo(endTime).withTableName("the_table")
  }
}
