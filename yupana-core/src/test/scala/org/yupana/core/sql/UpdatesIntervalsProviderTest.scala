package org.yupana.core.sql

import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time

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
      Some(Eq(FieldName("table"), Constant(TypedValue("some_table"))))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withTableName("some_table")
  }

  it should "support between time filter" in {
    createFilter(
      Some(
        BetweenCondition(
          FieldName("updated_at"),
          TypedValue(Time(startTime)),
          TypedValue(Time(endTime))
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withUpdatedAfter(startTime)
      .withUpdatedBefore(endTime)
  }

  it should "support updater filter" in {
    createFilter(
      Some(Eq(FieldName("updated_by"), Constant(TypedValue("somebody"))))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
  }

  it should "combine filters" in {

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(TypedValue("some_table"))),
            BetweenCondition(
              FieldName("updated_at"),
              TypedValue(Time(startTime)),
              TypedValue(Time(endTime))
            ),
            Eq(FieldName("updated_by"), Constant(TypedValue("somebody")))
          )
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withUpdatedBefore(endTime)
      .withUpdatedAfter(startTime)
      .withTableName("some_table")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(TypedValue("some_table"))),
            BetweenCondition(
              FieldName("recalculated_at"),
              TypedValue(Time(startTime)),
              TypedValue(Time(endTime))
            ),
            Eq(FieldName("updated_by"), Constant(TypedValue("somebody")))
          )
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withRecalculatedBefore(endTime)
      .withRecalculatedAfter(startTime)
      .withTableName("some_table")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(TypedValue("some_table"))),
            Ge(
              FieldName("recalculated_at"),
              Constant(TypedValue(Time(startTime)))
            ),
            Eq(FieldName("updated_by"), Constant(TypedValue("somebody")))
          )
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withRecalculatedAfter(startTime)
      .withTableName("some_table")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("TABLE"), Constant(TypedValue("some_table"))),
            BetweenCondition(
              FieldName("UpDated_at"),
              TypedValue(Time(startTime)),
              TypedValue(Time(endTime))
            ),
            Eq(FieldName("updaTed_by"), Constant(TypedValue("somebody")))
          )
        )
      )
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withBy("somebody")
      .withUpdatedBefore(endTime)
      .withUpdatedAfter(startTime)
      .withTableName("some_table")
  }

  it should "ignore unknown fields" in {
    createFilter(
      Some(Eq(FieldName("unknown_field"), Constant(TypedValue("unknown"))))
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
      Map(1 -> TypedValue(Time(startTime)), 2 -> TypedValue(Time(endTime)), 3 -> TypedValue("the_table"))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withUpdatedAfter(startTime)
      .withUpdatedBefore(endTime)
      .withTableName("the_table")

    createFilter(
      Some(
        And(
          Seq(
            Ge(FieldName("recalculated_at"), Constant(Placeholder(1))),
            Eq(FieldName("table"), Constant(Placeholder(2)))
          )
        )
      ),
      Map(1 -> TypedValue(Time(startTime)), 2 -> TypedValue("the_table"))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withRecalculatedAfter(startTime)
      .withTableName("the_table")
  }
}
