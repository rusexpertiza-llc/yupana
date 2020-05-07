package org.yupana.spark.rollup

import org.joda.time._
import org.scalatest.{ FlatSpec, Matchers }

class RollupCalculatorTest extends FlatSpec with Matchers {

  it should "slice time intervals" in {
    // empty
    RollupCalculator.sliceInterval(
      new Interval(new DateTime("2018-01-24"), new DateTime("2018-01-24")),
      DateTimeFieldType.dayOfYear
    ) shouldBe Seq.empty
    // good
    RollupCalculator.sliceInterval(
      new Interval(new DateTime("2018-01-24"), new DateTime("2018-01-28")),
      DateTimeFieldType.dayOfYear
    ) shouldBe Seq(
      new Interval(new DateTime("2018-01-24"), new DateTime("2018-01-25")),
      new Interval(new DateTime("2018-01-25"), new DateTime("2018-01-26")),
      new Interval(new DateTime("2018-01-26"), new DateTime("2018-01-27")),
      new Interval(new DateTime("2018-01-27"), new DateTime("2018-01-28"))
    )
    // bad (border not at start of a day, do not reject mod in this case, use not full last interval)
    RollupCalculator.sliceInterval(
      new Interval(new DateTime("2018-01-24"), new DateTime("2018-01-26T12:34:01")),
      DateTimeFieldType.dayOfYear
    ) shouldBe Seq(
      new Interval(new DateTime("2018-01-24"), new DateTime("2018-01-25")),
      new Interval(new DateTime("2018-01-25"), new DateTime("2018-01-26")),
      new Interval(new DateTime("2018-01-26"), new DateTime("2018-01-26T12:34:01"))
    )
    // another field (month)
    RollupCalculator.sliceInterval(
      new Interval(new DateTime("2018-01-01"), new DateTime("2018-03-02")),
      DateTimeFieldType.monthOfYear
    ) shouldBe Seq(
      new Interval(new DateTime("2018-01-01"), new DateTime("2018-02-01")),
      new Interval(new DateTime("2018-02-01"), new DateTime("2018-03-01")),
      new Interval(new DateTime("2018-03-01"), new DateTime("2018-03-02"))
    )
  }

  it should "correctly calculate invalid intervals" in {
    val daysInJan = RollupCalculator.sliceInterval(
      new Interval(new DateTime("2018-01-01"), new DateTime("2018-02-01")),
      DateTimeFieldType.dayOfYear
    )
    val invalidMarks = Seq(
      new DateTime("2018-01-03"),
      new DateTime("2018-01-04"), // these should be concatenated
      new DateTime("2018-01-23T12:00:01"),
      new DateTime("2019-01-23")
    ).map(_.getMillis) // out of total
    RollupCalculator.findInvalid(daysInJan, invalidMarks, new DateTime("2018-02-01").getMillis) shouldBe Seq(
      new Interval(new DateTime("2018-01-03"), new DateTime("2018-01-05")),
      new Interval(new DateTime("2018-01-23"), new DateTime("2018-01-24"))
    )
    RollupCalculator.findInvalid(daysInJan, invalidMarks, new DateTime("2018-01-27").getMillis) shouldBe Seq(
      new Interval(new DateTime("2018-01-03"), new DateTime("2018-01-05")),
      new Interval(new DateTime("2018-01-23"), new DateTime("2018-01-24")),
      new Interval(new DateTime("2018-01-27"), new DateTime("2018-02-01"))
    )
  }
}
