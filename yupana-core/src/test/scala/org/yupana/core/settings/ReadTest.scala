package org.yupana.core.settings

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time._
import scala.concurrent.duration.Duration

class ReadTest extends AnyFlatSpec with Matchers {

  "Read" should "parse numeric string values like \"1d\" to Double" in {
    read[Double](Double.MaxValue.toString) should be(Double.MaxValue)
    read[Double](Double.MinValue.toString) should be(Double.MinValue)
    read[Double]("1d") should be(1d)
  }

  it should "parse string values like \"P10M26D\" to Period" in {
    val startDate = LocalDate.of(2021, 2, 20)
    val endDate = LocalDate.of(2022, 1, 15)
    val period = Period.between(startDate, endDate)
    read[Period](period.toString) should be(period)
  }

  it should "parse numeric string values like \"1\" to Int" in {
    read[Int](Int.MaxValue.toString) should be(Int.MaxValue)
    read[Int](Int.MinValue.toString) should be(Int.MinValue)
    read[Int]("1") should be(1)
  }

  it should "parse string values like \" SomeString \" to String and trim it" in {
    val str = "SomeString"
    read[String](str) should be(str)
    read[String](str + " ") should be(str)
  }

  it should "parse comma separated string values like \"a, b, c\\, d\\, e, f\" to Sequence of String" in {
    val str = "a, b, c\\, d\\, e, f"
    val res = read[Seq[String]](str)
    res should contain theSameElementsInOrderAs List("a", "b", "c, d, e", "f")
  }

  it should "parse comma separated numeric string values like \"0, 1, 1, 2, 3, 3\" to Sequence of Numeric" in {
    val numStr = "0, 1, 1, 2, 3, 3"
    val numRes = read[Seq[Int]](numStr)
    numRes should contain theSameElementsInOrderAs List(0, 1, 1, 2, 3, 3)
  }

  it should "parse string values like \"100500 nanoseconds\" to Duration" in {
    val duration = Duration.fromNanos(100500L)
    read[Duration](duration.toString) should be(duration)
  }

  it should "parse comma separated string values like \"a, b, c\\, d, f, f\" to Set of String" in {
    val str = "a, b, c\\, d, f, f"
    val res = read[Set[String]](str)
    res should contain theSameElementsAs List("a", "b", "c, d", "f")
  }

  it should "parse comma separated numeric string values like \"0, 1, 1, 2, 3, 3\" to Set of Numeric" in {
    val numStr = "0, 1, 1, 2, 3, 3"
    val numRes = read[Set[Int]](numStr)
    numRes should contain theSameElementsAs List(0, 1, 2, 3)
  }

  it should "parse string values like \"true\" to Boolean" in {
    read[Boolean]("True") should be(true)
    read[Boolean]("true") should be(true)
    read[Boolean]("TRUE") should be(true)
    read[Boolean]("False") should be(false)
    read[Boolean]("false") should be(false)
    read[Boolean]("FALSE") should be(false)
  }

  it should "parse string values like \"2022-08-03T02:52:36.966931+02:00\" to OffsetDateTime" in {
    val zoneOffSet = ZoneOffset.of("+02:00")
    val offsetDateTime = OffsetDateTime.now(zoneOffSet)
    read[OffsetDateTime](offsetDateTime.toString) should be(offsetDateTime)
  }

  it should "parse numeric string values like \"1\" to Long" in {
    read[Long](Long.MaxValue.toString) should be(Long.MaxValue)
    read[Long](Long.MinValue.toString) should be(Long.MinValue)
    read[Long]("1") should be(1L)
  }

  it should "parse string values like \"2022-08-03T03:53:57.524762\" to LocalDateTime" in {
    val localDateTime = LocalDateTime.now
    read[LocalDateTime](localDateTime.toString) should be(localDateTime)
  }

  it should "parse string values like \"2022-08-03\" to LocalDate" in {
    val localDate = LocalDate.now
    read[LocalDate](localDate.toString) should be(localDate)
  }

  private def read[T](s: String)(implicit r: Read[T]): T = r.read(s)

}
