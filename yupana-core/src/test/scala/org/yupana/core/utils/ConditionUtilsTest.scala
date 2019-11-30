package org.yupana.core.utils

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.Dimension
import org.yupana.core.utils.ConditionMatchers.{ Equ, Neq }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConditionUtilsTest extends AnyFlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ConditionUtils.simplify" should "keep simple condition as is" in {
    val c = equ[String](dimension(Dimension("foo")), const("bar"))
    ConditionUtils.simplify(c) shouldEqual c
  }

  it should "flatten nested ANDs" in {
    val c = AndExpr(
      Seq(
        AndExpr(
          Seq(
            gt[String](dimension(Dimension("a")), dimension(Dimension("b"))),
            gt[String](dimension(Dimension("b")), const("c"))
          )
        ),
        AndExpr(Seq()).aux,
        gt[String](dimension(Dimension("c")), const("c")),
        AndExpr(Seq(AndExpr(Seq(gt[String](dimension(Dimension("d")), const("e"))))))
      )
    )
    ConditionUtils.simplify(c) shouldEqual and(
      gt[String](dimension(Dimension("a")), dimension(Dimension("b"))),
      gt[String](dimension(Dimension("b")), const("c")),
      gt[String](dimension(Dimension("c")), const("c")),
      gt[String](dimension(Dimension("d")), const("e"))
    )
  }

  it should "flatten nested ORs" in {
    val c = OrExpr(
      Seq(
        OrExpr(
          Seq(
            gt[String](dimension(Dimension("a")), dimension(Dimension("b"))),
            gt[String](dimension(Dimension("b")), const("c"))
          )
        ),
        OrExpr(Seq()).aux,
        gt[String](dimension(Dimension("c")), const("c")),
        OrExpr(Seq(OrExpr(Seq(gt[String](dimension(Dimension("d")), const("e"))))))
      )
    )
    ConditionUtils.simplify(c) shouldEqual OrExpr(
      Seq(
        gt[String](dimension(Dimension("a")), dimension(Dimension("b"))),
        gt[String](dimension(Dimension("b")), const("c")),
        gt[String](dimension(Dimension("c")), const("c")),
        gt[String](dimension(Dimension("d")), const("e"))
      )
    )
  }

  it should "process ANDs in OR" in {
    val c = or(
      and(
        and(equ[String](dimension(Dimension("x")), const("y"))),
        lt(dimension(Dimension("z")), const("zz"))
      ),
      neq[String](dimension(Dimension("a")), const("a")),
      or(
        neq[String](dimension(Dimension("b")), const("d")),
        neq[String](dimension(Dimension("c")), const("e"))
      )
    )

    ConditionUtils.simplify(c) shouldEqual or(
      and(
        equ[String](dimension(Dimension("x")), const("y")),
        lt(dimension(Dimension("z")), const("zz"))
      ),
      neq[String](dimension(Dimension("a")), const("a")),
      neq[String](dimension(Dimension("b")), const("d")),
      neq[String](dimension(Dimension("c")), const("e"))
    )
  }

  ignore should "handle OR in AND" in {
    val c = and(
      or(
        equ[String](dimension(Dimension("a")), const("a")),
        equ[String](dimension(Dimension("b")), const("b"))
      ),
      or(
        equ[String](dimension(Dimension("c")), const("c")),
        equ[String](dimension(Dimension("d")), const("d"))
      )
    )

    ConditionUtils.simplify(c) shouldEqual or(
      and(
        equ[String](dimension(Dimension("a")), const("a")),
        equ[String](dimension(Dimension("c")), const("c"))
      ),
      and(
        equ[String](dimension(Dimension("a")), const("a")),
        equ[String](dimension(Dimension("d")), const("d"))
      ),
      and(
        equ[String](dimension(Dimension("b")), const("b")),
        equ[String](dimension(Dimension("c")), const("c"))
      ),
      and(
        equ[String](dimension(Dimension("b")), const("b")),
        equ[String](dimension(Dimension("d")), const("d"))
      )
    )
  }

  "ConditionUtils.flatMap" should "apply function to condition" in {
    val c = in(dimension(Dimension("x")), Set("a", "b", "c"))

    ConditionUtils.flatMap(c) {
      case InExpr(DimensionExpr(Dimension("x", None)), _) => InExpr(DimensionExpr(Dimension("y", None)), Set("x"))
      case _                                              => ConstantExpr(true)
    } shouldEqual in(dimension(Dimension("y")), Set("x"))
  }

  it should "return empty condition if function is not defined for condition" in {
    val c = in(dimension(Dimension("x")), Set("a", "b", "c"))

    ConditionUtils.flatMap(c) {
      case InExpr(DimensionExpr(Dimension("y", None)), _) => InExpr(DimensionExpr(Dimension("z", None)), Set("x"))
      case _                                              => ConstantExpr(true)
    } shouldEqual ConstantExpr(true)
  }

  it should "handle AND and OR conditions" in {
    val c = or(
      equ(dimension(Dimension("x")), const("a")),
      and(neq(dimension(Dimension("y")), const("b")), gt(dimension(Dimension("x")), const("9")))
    )

    ConditionUtils.flatMap(c) {
      case x @ BinaryOperationExpr(f, DimensionExpr(_), _) if f.name == "==" || f.name == "!=" => x
      case _                                                                                   => ConstantExpr(true)
    } shouldEqual or(equ(dimension(Dimension("x")), const("a")), neq(dimension(Dimension("y")), const("b")))
  }

  "ConditionUtils.merge" should "combine two AND conditions" in {
    val a = and(equ(dimension(Dimension("x")), const("foo")), equ(dimension(Dimension("y")), const("bar")))
    val b = and(equ(dimension(Dimension("a")), const("baz")), in(dimension(Dimension("b")), Set("a", "b", "c")))

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(Dimension("x")), const("foo")),
      equ(dimension(Dimension("y")), const("bar")),
      equ(dimension(Dimension("a")), const("baz")),
      in(dimension(Dimension("b")), Set("a", "b", "c"))
    )
  }

  it should "combine AND and comparison" in {
    val a = and(equ(dimension(Dimension("x")), const("foo")), equ(dimension(Dimension("y")), const("bar")))
    val b = gt(dimension(Dimension("z")), const("qux"))

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(Dimension("x")), const("foo")),
      equ(dimension(Dimension("y")), const("bar")),
      gt(dimension(Dimension("z")), const("qux"))
    )
  }

  it should "avoid duplicates" in {
    val a = and(equ(dimension(Dimension("x")), const("bar")), gt(time, const(Time(100))), lt(time, const(Time(500))))
    val b =
      and(in(dimension(Dimension("y")), Set("foo", "bar")), gt(time, const(Time(100))), lt(time, const(Time(500))))

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(Dimension("x")), const("bar")),
      in(dimension(Dimension("y")), Set("foo", "bar")),
      gt(time, const(Time(100))),
      lt(time, const(Time(500)))
    )
  }

  "ConditionUtils.split" should "split simple condition by predicate" in {
    val c = equ(dimension(Dimension("foo")), const("44"))

    ConditionUtils.split(c) {
      case Equ(DimensionExpr(Dimension("foo", None)), ConstantExpr(_)) => true
      case _                                                           => false
    } shouldBe ((c, ConstantExpr(true)))

    ConditionUtils.split(c) {
      case BinaryOperationExpr(f, DimensionExpr(Dimension("bar", None)), ConstantExpr(_)) if f.name == "==" =>
        true
      case _ => false
    } shouldBe ((ConstantExpr(true), c))
  }

  it should "split AND conditions" in {
    val c = and(
      equ(dimension(Dimension("x")), const("b")),
      gt(dimension(Dimension("y")), dimension(Dimension("x"))),
      neq(dimension(Dimension("y")), const("z"))
    )

    val (l, r) = ConditionUtils.split(c) {
      case Neq(DimensionExpr(Dimension("y", None)), ConstantExpr(_)) => true
      case _                                                         => false
    }

    l shouldEqual neq(dimension(Dimension("y")), const("z"))
    r shouldEqual and(
      equ(dimension(Dimension("x")), const("b")),
      gt(dimension(Dimension("y")), dimension(Dimension("x")))
    )
  }

  it should "support nested ANDs and ORs" in {
    val c = or(
      and(
        equ(dimension(Dimension("x")), const("b")),
        gt(dimension(Dimension("y")), dimension(Dimension("x")))
      ),
      and(
        neq(dimension(Dimension("y")), const("z")),
        equ(dimension(Dimension("x")), const("a"))
      ),
      lt(dimension(Dimension("z")), const("c"))
    )

    val (l, r) = ConditionUtils.split(c) {
      case Equ(DimensionExpr(Dimension("x", None)), ConstantExpr(_)) => true
      case _                                                         => false
    }

    l shouldEqual or(equ(dimension(Dimension("x")), const("b")), equ(dimension(Dimension("x")), const("a")))
    r shouldEqual or(
      gt(dimension(Dimension("y")), dimension(Dimension("x"))),
      neq(dimension(Dimension("y")), const("z")),
      lt(dimension(Dimension("z")), const("c"))
    )
  }
}
