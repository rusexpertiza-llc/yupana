package org.yupana.core

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.utils.RussianTokenizer
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class ConstantCalculatorTest extends AnyFlatSpec with Matchers with OptionValues {

  private val calculator = new ConstantCalculator(RussianTokenizer)

  import org.yupana.api.query.syntax.All._

  "Constant calculator" should "Evaluate constants" in {
    import org.yupana.api.query.syntax.All

    calculator.evaluateConstant(plus(const(2), times(const(2), const(2)))) shouldEqual 6
    calculator.evaluateConstant(divInt(All.length(const("9 letters")), const(3))) shouldEqual 3
    calculator.evaluateConstant(All.not(contains(const(Seq(1L, 2L, 3L)), const(5L)))) shouldEqual true
  }

  it should "Evaluate different time functions" in {
    val t = const(Time(LocalDateTime.of(2020, 10, 21, 11, 36, 42)))

    calculator.evaluateConstant(extractYear(t)) shouldEqual 2020
    calculator.evaluateConstant(extractMonth(t)) shouldEqual 10
    calculator.evaluateConstant(extractDay(t)) shouldEqual 21
    calculator.evaluateConstant(extractHour(t)) shouldEqual 11
    calculator.evaluateConstant(extractMinute(t)) shouldEqual 36
    calculator.evaluateConstant(extractSecond(t)) shouldEqual 42

    calculator.evaluateConstant(truncYear(t)) shouldEqual Time(LocalDateTime.of(2020, 1, 1, 0, 0))
    calculator.evaluateConstant(truncQuarter(t)) shouldEqual Time(LocalDateTime.of(2020, 10, 1, 0, 0))
    calculator.evaluateConstant(truncMonth(t)) shouldEqual Time(LocalDateTime.of(2020, 10, 1, 0, 0))
    calculator.evaluateConstant(truncDay(t)) shouldEqual Time(LocalDateTime.of(2020, 10, 21, 0, 0))
    calculator.evaluateConstant(truncWeek(t)) shouldEqual Time(LocalDateTime.of(2020, 10, 19, 0, 0))
    calculator.evaluateConstant(truncHour(t)) shouldEqual Time(LocalDateTime.of(2020, 10, 21, 11, 0))
    calculator.evaluateConstant(truncMinute(t)) shouldEqual Time(
      LocalDateTime.of(2020, 10, 21, 11, 36)
    )
    calculator.evaluateConstant(truncSecond(t)) shouldEqual Time(
      LocalDateTime.of(2020, 10, 21, 11, 36, 42)
    )
  }

  it should "Evaluate string functions" in {
    calculator.evaluateConstant(upper(const("Вкусная водичка №7"))) shouldEqual "ВКУСНАЯ ВОДИЧКА №7"
    calculator.evaluateConstant(lower(const("Вкусная водичка №7"))) shouldEqual "вкусная водичка №7"

    calculator.evaluateConstant(split(const("Вкусная водичка №7"))) should contain theSameElementsInOrderAs Seq(
      "Вкусная",
      "водичка",
      "7"
    )

    calculator
      .evaluateConstant(tokens(const("Вкусная водичка №7"))) should contain theSameElementsInOrderAs Seq(
      "vkusn",
      "vodichk",
      "№7"
    )
  }

  it should "Evaluate array functions" in {
    calculator.evaluateConstant(arrayLength(const(Seq(1, 2, 3)))) shouldEqual 3
    calculator.evaluateConstant(arrayToString(const(Seq(1, 2, 3, 4)))) shouldEqual "1, 2, 3, 4"
    calculator.evaluateConstant(arrayToString(const(Seq("1", "2", "4")))) shouldEqual "1, 2, 4"
    calculator.evaluateConstant(
      ArrayTokensExpr(const(Seq("красная вода", "соленые яблоки")))
    ) should contain theSameElementsAs Seq(
      "krasn",
      "vod",
      "solen",
      "yablok"
    )

    calculator.evaluateConstant(
      arrayToString(array(const(1), plus(const(2), const(3)), const(4)))
    ) shouldEqual "1, 5, 4"

    calculator.evaluateConstant(containsAll(array(const(1), const(2), const(3)), const(Seq(2, 3)))) shouldBe true
    calculator.evaluateConstant(containsAll(array(const(1), const(2), const(3)), const(Seq(2, 4)))) shouldBe false

    calculator.evaluateConstant(containsAny(array(const(1), const(2), const(3)), const(Seq(2, 3)))) shouldBe true
    calculator.evaluateConstant(containsAny(const(Seq(1, 2, 3)), const(Seq(2, 4)))) shouldBe true

    calculator.evaluateConstant(containsSame(array(const(1), const(2)), const(Seq(1, 2)))) shouldBe true
    calculator.evaluateConstant(containsSame(array(const("1"), const("2")), const(Seq("2", "1")))) shouldBe true
    calculator.evaluateConstant(containsSame(const(Seq(1, 2, 2)), const(Seq(1, 2)))) shouldBe false
  }
}
