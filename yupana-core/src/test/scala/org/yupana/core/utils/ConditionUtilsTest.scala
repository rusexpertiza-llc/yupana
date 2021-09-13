package org.yupana.core.utils

import org.yupana.api.query._
import org.yupana.api.schema.RawDimension
import org.yupana.api.utils.ConditionMatchers.{ EqString, NeqString }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConditionUtilsTest extends AnyFlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ConditionUtils.filter" should "split filter condition by predicate" in {
    val c = equ(dimension(RawDimension[Int]("foo")), const(44))

    ConditionUtils.filter(c) {
      case EqExpr(DimensionExpr(RawDimension("foo")), ConstantExpr(_)) => true
      case _                                                           => false
    } shouldBe c

    ConditionUtils.filter(c) {
      case EqExpr(DimensionExpr(RawDimension("bar")), ConstantExpr(_)) => true
      case _                                                           => false
    } shouldBe ConstantExpr(true)
  }

  it should "filter AND conditions" in {
    val c = and(
      equ(dimension(RawDimension[Int]("x")), const(1)),
      gt(dimension(RawDimension[Int]("y")), dimension(RawDimension[Int]("x"))),
      neq(dimension(RawDimension[Int]("y")), const(2))
    )

    val f = ConditionUtils.filter(c) {
      case NeqString(DimensionExpr(RawDimension("y")), ConstantExpr(_)) => true
      case _                                                            => false
    }

    f shouldEqual neq(dimension(RawDimension[Int]("y")), const(2))
  }

  it should "support nested ANDs and ORs" in {
    val c = or(
      and(
        equ(dimension(RawDimension[Int]("x")), const(2)),
        gt(dimension(RawDimension[Int]("y")), dimension(RawDimension[Int]("x")))
      ),
      and(
        neq(dimension(RawDimension[Int]("y")), const(3)),
        equ(dimension(RawDimension[Int]("x")), const(1))
      ),
      lt(dimension(RawDimension[Long]("z")), const(12345L))
    )

    val f = ConditionUtils.filter(c) {
      case EqString(DimensionExpr(RawDimension("x")), ConstantExpr(_)) => true
      case _                                                           => false
    }

    f shouldEqual or(
      equ(dimension(RawDimension[Int]("x")), const(2)),
      equ(dimension(RawDimension[Int]("x")), const(1))
    )
  }
}
