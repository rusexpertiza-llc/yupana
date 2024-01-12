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
      .withUpdatedAfter(startTime)
      .withUpdatedBefore(endTime)
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
      .withUpdatedBefore(endTime)
      .withUpdatedAfter(startTime)
      .withTableName("some_table")

    createFilter(
      Some(
        And(
          Seq(
            Eq(FieldName("table"), Constant(StringValue("some_table"))),
            BetweenCondition(
              FieldName("recalculated_at"),
              TimestampValue(startTime),
              TimestampValue(endTime)
            ),
            Eq(FieldName("updated_by"), Constant(StringValue("somebody")))
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
            Eq(FieldName("table"), Constant(StringValue("some_table"))),
            Ge(
              FieldName("recalculated_at"),
              Constant(TimestampValue(startTime))
            ),
            Eq(FieldName("updated_by"), Constant(StringValue("somebody")))
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
            Eq(FieldName("TABLE"), Constant(StringValue("some_table"))),
            BetweenCondition(
              FieldName("UpDated_at"),
              TimestampValue(startTime),
              TimestampValue(endTime)
            ),
            Eq(FieldName("updaTed_by"), Constant(StringValue("somebody")))
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
      Map(1 -> TimestampValue(startTime), 2 -> StringValue("the_table"))
    ).value shouldBe UpdatesIntervalsFilter.empty
      .withRecalculatedAfter(startTime)
      .withTableName("the_table")
  }
}
