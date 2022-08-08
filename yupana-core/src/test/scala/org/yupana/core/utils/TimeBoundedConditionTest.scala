package org.yupana.core.utils

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.core.{ ConstantCalculator, TestDims, TestTableFields }
import org.yupana.utils.RussianTokenizer

import java.time.LocalDateTime

class TimeBoundedConditionTest extends AnyFlatSpec with Matchers with OptionValues {

  import org.yupana.api.query.syntax.All._

  private val calculator = new ConstantCalculator(RussianTokenizer)

  "TimeBoundedCondition" should "handle simple AND conditions" in {
    val from = Time(LocalDateTime.now().minusDays(1))
    val to = Time(LocalDateTime.now())

    val condition = and(ge(time, const(from)), lt(time, const(to)), equ(dimension(TestDims.DIM_A), const("value")))

    val tbcs = TimeBoundedCondition(calculator, condition)

    tbcs should have size 1
    val tbc = tbcs.head

    tbc.from.value shouldEqual from.millis
    tbc.to.value shouldEqual to.millis
    tbc.conditions should contain theSameElementsAs List(equ(dimension(TestDims.DIM_A), const("value")))
  }

  it should "handle ANDs in OR" in {
    val from1 = Time(LocalDateTime.now().minusDays(1))
    val to1 = Time(LocalDateTime.now())

    val from2 = Time(LocalDateTime.now().minusDays(5))
    val to2 = Time(LocalDateTime.now().minusDays(3))

    val condition = or(
      and(lt(time, const(to1)), ge(time, const(from1)), equ(dimension(TestDims.DIM_A), const("value"))),
      and(gt(time, const(from2)), le(time, const(to2)), in(metric(TestTableFields.TEST_FIELD), Set(1d, 2d)))
    )

    val tbcs = TimeBoundedCondition(calculator, condition)

    tbcs should have size 2
    val tbc1 = tbcs(0)
    val tbc2 = tbcs(1)

    tbc1.from.value shouldEqual from1.millis
    tbc1.to.value shouldEqual to1.millis
    tbc1.conditions should contain theSameElementsAs List(equ(dimension(TestDims.DIM_A), const("value")))

    tbc2.from.value shouldEqual from2.millis + 1
    tbc2.to.value shouldEqual to2.millis + 1
    tbc2.conditions should contain theSameElementsAs List(in(metric(TestTableFields.TEST_FIELD), Set(1d, 2d)))
  }

  it should "fail if there conditions without time bound" in {
    val from = Time(LocalDateTime.now().minusMonths(1))
    val to = Time(LocalDateTime.now().minusWeeks(2))

    val condition = or(
      and(ge(time, const(from)), lt(time, const(to)), equ(dimension(TestDims.DIM_A), const("x"))),
      and(ge(time, const(from)), equ(dimension(TestDims.DIM_A), const("y")))
    )

    an[IllegalArgumentException] should be thrownBy TimeBoundedCondition(calculator, condition)
  }
}