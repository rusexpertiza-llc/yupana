package org.yupana.core.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, RawDimension }
import org.yupana.api.utils.ConditionMatchers.InString

class ConditionUtilsTest extends AnyFlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ConditionUtils.flatMap" should "apply function to condition" in {
    val c = in(dimension(DictionaryDimension("x")), Set("a", "b", "c"))

    ConditionUtils.flatMap(c) {
      case InString(DimensionExpr(DictionaryDimension("x", None)), _) =>
        in(dimension(DictionaryDimension("y", None)), Set("x"))

      case _ => ConstantExpr(true)
    } shouldEqual in(dimension(DictionaryDimension("y")), Set("x"))
  }

  it should "return empty condition if function is not defined for condition" in {
    val c = in(dimension(RawDimension[Int]("x")), Set(1, 2, 3))

    ConditionUtils.flatMap(c) {
      case InExpr(DimensionExpr(RawDimension("y")), _) =>
        in(dimension(DictionaryDimension("z", None)), Set("x"))
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
}
