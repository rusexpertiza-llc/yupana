package org.yupana.core.utils

import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, RawDimension }
import org.yupana.api.utils.ConditionMatchers.{ EqString, InString, NeqString }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConditionUtilsTest extends AnyFlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ConditionUtils.flatMap" should "apply function to condition" in {
    val c = in(dimension(DictionaryDimension("x")), Set("a", "b", "c"))

    ConditionUtils.flatMap(c) {
      case InString(DimensionExpr(DictionaryDimension("x", None)), _) =>
        InExpr(DimensionExpr(DictionaryDimension("y", None)), Set("x"))

      case _ => ConstantExpr(true)
    } shouldEqual in(dimension(DictionaryDimension("y")), Set("x"))
  }

  it should "return empty condition if function is not defined for condition" in {
    val c = in(dimension(RawDimension[Int]("x")), Set(1, 2, 3))

    ConditionUtils.flatMap(c) {
      case InExpr(DimensionExpr(RawDimension("y")), _) =>
        InExpr(DimensionExpr(DictionaryDimension("z", None)), Set("x"))
      case _ => ConstantExpr(true)
    } shouldEqual ConstantExpr(true)
  }

  it should "handle AND and OR conditions" in {
    val c = or(
      equ(dimension(RawDimension[Int]("x")), const(66)),
      and(neq(dimension(DictionaryDimension("y")), const("b")), gt(dimension(RawDimension[Int]("x")), const(9)))
    )

    ConditionUtils.flatMap(c) {
      case x @ EqExpr(DimensionExpr(_), _)  => x
      case x @ NeqExpr(DimensionExpr(_), _) => x
      case _                                => ConstantExpr(true)
    } shouldEqual or(
      equ(dimension(RawDimension[Int]("x")), const(66)),
      neq(dimension(DictionaryDimension("y")), const("b"))
    )
  }

  "ConditionUtils.split" should "split simple condition by predicate" in {
    val c = equ(dimension(RawDimension[Int]("foo")), const(44))

    ConditionUtils.split(c) {
      case EqExpr(DimensionExpr(RawDimension("foo")), ConstantExpr(_)) => true
      case _                                                           => false
    } shouldBe ((c, ConstantExpr(true)))

    ConditionUtils.split(c) {
      case EqExpr(DimensionExpr(RawDimension("bar")), ConstantExpr(_)) => true
      case _                                                           => false
    } shouldBe ((ConstantExpr(true), c))
  }

  it should "split AND conditions" in {
    val c = and(
      equ(dimension(DictionaryDimension("x")), const("b")),
      gt(dimension(DictionaryDimension("y")), dimension(DictionaryDimension("x"))),
      neq(dimension(DictionaryDimension("y")), const("z"))
    )

    val (l, r) = ConditionUtils.split(c) {
      case NeqString(DimensionExpr(DictionaryDimension("y", None)), ConstantExpr(_)) => true
      case _                                                                         => false
    }

    l shouldEqual neq(dimension(DictionaryDimension("y")), const("z"))
    r shouldEqual and(
      equ(dimension(DictionaryDimension("x")), const("b")),
      gt(dimension(DictionaryDimension("y")), dimension(DictionaryDimension("x")))
    )
  }

  it should "support nested ANDs and ORs" in {
    val c = or(
      and(
        equ(dimension(DictionaryDimension("x")), const("b")),
        gt(dimension(DictionaryDimension("y")), dimension(DictionaryDimension("x")))
      ),
      and(
        neq(dimension(DictionaryDimension("y")), const("z")),
        equ(dimension(DictionaryDimension("x")), const("a"))
      ),
      lt(dimension(RawDimension[Long]("z")), const(12345L))
    )

    val (l, r) = ConditionUtils.split(c) {
      case EqString(DimensionExpr(DictionaryDimension("x", None)), ConstantExpr(_)) => true
      case _                                                                        => false
    }

    l shouldEqual or(
      equ(dimension(DictionaryDimension("x")), const("b")),
      equ(dimension(DictionaryDimension("x")), const("a"))
    )
    r shouldEqual or(
      gt(dimension(DictionaryDimension("y")), dimension(DictionaryDimension("x"))),
      neq(dimension(DictionaryDimension("y")), const("z")),
      lt(dimension(RawDimension[Long]("z")), const(12345L))
    )
  }
}
