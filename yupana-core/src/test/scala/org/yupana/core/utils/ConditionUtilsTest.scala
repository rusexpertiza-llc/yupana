package org.yupana.core.utils

import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, RawDimension }
import org.yupana.core.utils.ConditionMatchers.{ Equ, Neq }

class ConditionUtilsTest extends FlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ConditionUtils.simplify" should "keep simple condition as is" in {
    val c = equ[String](dimension(DictionaryDimension("foo")), const("bar"))
    ConditionUtils.simplify(c) shouldEqual c
  }

  it should "flatten nested ANDs" in {
    val c = AndExpr(
      Seq(
        AndExpr(
          Seq(
            gt[String](dimension(DictionaryDimension("a")), dimension(DictionaryDimension("b"))),
            gt[String](dimension(DictionaryDimension("b")), const("c"))
          )
        ),
        AndExpr(Seq()).aux,
        gt(dimension(RawDimension[Int]("c")), const(42)),
        AndExpr(Seq(AndExpr(Seq(gt(dimension(RawDimension[Long]("d")), const(5L))))))
      )
    )
    ConditionUtils.simplify(c) shouldEqual and(
      gt[String](dimension(DictionaryDimension("a")), dimension(DictionaryDimension("b"))),
      gt[String](dimension(DictionaryDimension("b")), const("c")),
      gt(dimension(RawDimension[Int]("c")), const(42)),
      gt(dimension(RawDimension[Long]("d")), const(5L))
    )
  }

  it should "flatten nested ORs" in {
    val c = OrExpr(
      Seq(
        OrExpr(
          Seq(
            gt[String](dimension(DictionaryDimension("a")), dimension(DictionaryDimension("b"))),
            gt[String](dimension(DictionaryDimension("b")), const("c"))
          )
        ),
        OrExpr(Seq()).aux,
        gt[String](dimension(DictionaryDimension("c")), const("c")),
        OrExpr(Seq(OrExpr(Seq(gt[String](dimension(DictionaryDimension("d")), const("e"))))))
      )
    )
    ConditionUtils.simplify(c) shouldEqual OrExpr(
      Seq(
        gt[String](dimension(DictionaryDimension("a")), dimension(DictionaryDimension("b"))),
        gt[String](dimension(DictionaryDimension("b")), const("c")),
        gt[String](dimension(DictionaryDimension("c")), const("c")),
        gt[String](dimension(DictionaryDimension("d")), const("e"))
      )
    )
  }

  it should "process ANDs in OR" in {
    val c = or(
      and(
        and(equ(dimension(RawDimension[Int]("x")), const(11))),
        lt(dimension(RawDimension[Int]("z")), const(22))
      ),
      neq(dimension(RawDimension[Int]("a")), const(12)),
      or(
        neq(dimension(RawDimension[Long]("b")), const(2L)),
        neq(dimension(RawDimension[Long]("c")), const(5L))
      )
    )

    ConditionUtils.simplify(c) shouldEqual or(
      and(
        equ(dimension(RawDimension[Int]("x")), const(11)),
        lt(dimension(RawDimension[Int]("z")), const(22))
      ),
      neq(dimension(RawDimension[Int]("a")), const(12)),
      neq(dimension(RawDimension[Long]("b")), const(2L)),
      neq(dimension(RawDimension[Long]("c")), const(5L))
    )
  }

  ignore should "handle OR in AND" in {
    val c = and(
      or(
        equ[String](dimension(DictionaryDimension("a")), const("a")),
        equ[String](dimension(DictionaryDimension("b")), const("b"))
      ),
      or(
        equ[String](dimension(DictionaryDimension("c")), const("c")),
        equ[String](dimension(DictionaryDimension("d")), const("d"))
      )
    )

    ConditionUtils.simplify(c) shouldEqual or(
      and(
        equ[String](dimension(DictionaryDimension("a")), const("a")),
        equ[String](dimension(DictionaryDimension("c")), const("c"))
      ),
      and(
        equ[String](dimension(DictionaryDimension("a")), const("a")),
        equ[String](dimension(DictionaryDimension("d")), const("d"))
      ),
      and(
        equ[String](dimension(DictionaryDimension("b")), const("b")),
        equ[String](dimension(DictionaryDimension("c")), const("c"))
      ),
      and(
        equ[String](dimension(DictionaryDimension("b")), const("b")),
        equ[String](dimension(DictionaryDimension("d")), const("d"))
      )
    )
  }

  "ConditionUtils.flatMap" should "apply function to condition" in {
    val c = in(dimension(DictionaryDimension("x")), Set("a", "b", "c"))

    ConditionUtils.flatMap(c) {
      case InExpr(DimensionExpr(DictionaryDimension("x", None)), _) =>
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
      case x @ BinaryOperationExpr(f, DimensionExpr(_), _) if f.name == "==" || f.name == "!=" => x
      case _                                                                                   => ConstantExpr(true)
    } shouldEqual or(
      equ(dimension(RawDimension[Int]("x")), const(66)),
      neq(dimension(DictionaryDimension("y")), const("b"))
    )
  }

  "ConditionUtils.merge" should "combine two AND conditions" in {
    val a = and(
      equ(dimension(DictionaryDimension("x")), const("foo")),
      equ(dimension(DictionaryDimension("y")), const("bar"))
    )
    val b = and(
      equ(dimension(DictionaryDimension("a")), const("baz")),
      in(dimension(DictionaryDimension("b")), Set("a", "b", "c"))
    )

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(DictionaryDimension("x")), const("foo")),
      equ(dimension(DictionaryDimension("y")), const("bar")),
      equ(dimension(DictionaryDimension("a")), const("baz")),
      in(dimension(DictionaryDimension("b")), Set("a", "b", "c"))
    )
  }

  it should "combine AND and comparison" in {
    val a = and(
      equ(dimension(DictionaryDimension("x")), const("foo")),
      equ(dimension(DictionaryDimension("y")), const("bar"))
    )
    val b = gt(dimension(DictionaryDimension("z")), const("qux"))

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(DictionaryDimension("x")), const("foo")),
      equ(dimension(DictionaryDimension("y")), const("bar")),
      gt(dimension(DictionaryDimension("z")), const("qux"))
    )
  }

  it should "avoid duplicates" in {
    val a = and(
      equ(dimension(DictionaryDimension("x")), const("bar")),
      gt(time, const(Time(100))),
      lt(time, const(Time(500)))
    )
    val b =
      and(in(dimension(RawDimension[Int]("y")), Set(1, 2)), gt(time, const(Time(100))), lt(time, const(Time(500))))

    ConditionUtils.merge(a, b) shouldEqual and(
      equ(dimension(DictionaryDimension("x")), const("bar")),
      in(dimension(RawDimension[Int]("y")), Set(1, 2)),
      gt(time, const(Time(100))),
      lt(time, const(Time(500)))
    )
  }

  "ConditionUtils.split" should "split simple condition by predicate" in {
    val c = equ(dimension(RawDimension[Int]("foo")), const(44))

    ConditionUtils.split(c) {
      case Equ(DimensionExpr(RawDimension("foo")), ConstantExpr(_)) => true
      case _                                                        => false
    } shouldBe ((c, ConstantExpr(true)))

    ConditionUtils.split(c) {
      case Equ(DimensionExpr(RawDimension("bar")), ConstantExpr(_)) => true
      case _                                                        => false
    } shouldBe ((ConstantExpr(true), c))
  }

  it should "split AND conditions" in {
    val c = and(
      equ(dimension(DictionaryDimension("x")), const("b")),
      gt(dimension(DictionaryDimension("y")), dimension(DictionaryDimension("x"))),
      neq(dimension(DictionaryDimension("y")), const("z"))
    )

    val (l, r) = ConditionUtils.split(c) {
      case Neq(DimensionExpr(DictionaryDimension("y", None)), ConstantExpr(_)) => true
      case _                                                                   => false
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
      case Equ(DimensionExpr(DictionaryDimension("x", None)), ConstantExpr(_)) => true
      case _                                                                   => false
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
