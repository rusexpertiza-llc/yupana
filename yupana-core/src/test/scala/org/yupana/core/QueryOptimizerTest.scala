package org.yupana.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.{ AndExpr, OrExpr }
import org.yupana.api.schema.{ DictionaryDimension, RawDimension }

class QueryOptimizerTest extends AnyFlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  "QueryOptimizer.simplifyCondition" should "keep simple condition as is" in {
    val c = equ[String](dimension(DictionaryDimension("foo")), const("bar"))
    QueryOptimizer.simplifyCondition(c) shouldEqual c
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
        AndExpr(Seq()),
        gt(dimension(RawDimension[Int]("c")), const(42)),
        AndExpr(Seq(AndExpr(Seq(gt(dimension(RawDimension[Long]("d")), const(5L))))))
      )
    )
    QueryOptimizer.simplifyCondition(c) shouldEqual and(
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
        OrExpr(Seq()),
        gt[String](dimension(DictionaryDimension("c")), const("c")),
        OrExpr(Seq(OrExpr(Seq(gt[String](dimension(DictionaryDimension("d")), const("e"))))))
      )
    )
    QueryOptimizer.simplifyCondition(c) shouldEqual OrExpr(
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

    QueryOptimizer.simplifyCondition(c) shouldEqual or(
      and(
        equ(dimension(RawDimension[Int]("x")), const(11)),
        lt(dimension(RawDimension[Int]("z")), const(22))
      ),
      neq(dimension(RawDimension[Int]("a")), const(12)),
      neq(dimension(RawDimension[Long]("b")), const(2L)),
      neq(dimension(RawDimension[Long]("c")), const(5L))
    )
  }

  it should "handle nested ANDs in OR" in {
    // (A && B || C && D) && E => (A && B && E) || (C && D && E)
    val c =
      and(
        or(
          and(ge(dimension(RawDimension[Int]("a")), const(1)), lt(dimension(RawDimension[Int]("a")), const(10))),
          and(ge(dimension(RawDimension[Int]("a")), const(21)), lt(dimension(RawDimension[Int]("a")), const(30)))
        ),
        equ(dimension(RawDimension[Int]("b")), const(42))
      )

    QueryOptimizer.simplifyCondition(c) shouldEqual or(
      and(
        ge(dimension(RawDimension[Int]("a")), const(1)),
        lt(dimension(RawDimension[Int]("a")), const(10)),
        equ(dimension(RawDimension[Int]("b")), const(42))
      ),
      and(
        ge(dimension(RawDimension[Int]("a")), const(21)),
        lt(dimension(RawDimension[Int]("a")), const(30)),
        equ(dimension(RawDimension[Int]("b")), const(42))
      )
    )
  }

  it should "handle OR in AND" in {
    val c = and(
      or(
        or(equ[String](dimension(DictionaryDimension("a")), const("a"))),
        equ[String](dimension(DictionaryDimension("b")), const("b"))
      ),
      or(
        or(
          equ[String](dimension(DictionaryDimension("c")), const("c")),
          equ[String](dimension(DictionaryDimension("d")), const("d"))
        )
      )
    )

    val actual = QueryOptimizer.simplifyCondition(c)

    actual shouldEqual or(
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
}
