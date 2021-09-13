package org.yupana.core

import org.yupana.api.query.{ AndExpr, OrExpr }
import org.yupana.api.schema.RawDimension
import org.yupana.utils.RussianTokenizer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryOptimizerTest extends AnyFlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  private val calculator = new ConstantCalculator(RussianTokenizer)

  "QueryOptimizer.simplifyCondition" should "keep simple condition as is" in {
    val c = equ[Int](dimension(RawDimension[Int]("foo")), const(1))
    QueryOptimizer.simplifyCondition(c) shouldEqual c
  }

  it should "flatten nested ANDs" in {
    val c = AndExpr(
      Seq(
        AndExpr(
          Seq(
            gt[Int](dimension(RawDimension[Int]("a")), dimension(RawDimension[Int]("b"))),
            gt[Int](dimension(RawDimension[Int]("b")), const(3))
          )
        ),
        AndExpr(Seq()),
        gt(dimension(RawDimension[Int]("c")), const(42)),
        AndExpr(Seq(AndExpr(Seq(gt(dimension(RawDimension[Long]("d")), const(5L))))))
      )
    )
    QueryOptimizer.simplifyCondition(c) shouldEqual and(
      gt[Int](dimension(RawDimension[Int]("a")), dimension(RawDimension[Int]("b"))),
      gt[Int](dimension(RawDimension[Int]("b")), const(3)),
      gt(dimension(RawDimension[Int]("c")), const(42)),
      gt(dimension(RawDimension[Long]("d")), const(5L))
    )
  }

  it should "flatten nested ORs" in {
    val c = OrExpr(
      Seq(
        OrExpr(
          Seq(
            gt[Int](dimension(RawDimension[Int]("a")), dimension(RawDimension[Int]("b"))),
            gt[Int](dimension(RawDimension[Int]("b")), const(2))
          )
        ),
        OrExpr(Seq()),
        gt[Int](dimension(RawDimension[Int]("c")), const(2)),
        OrExpr(Seq(OrExpr(Seq(gt[Int](dimension(RawDimension[Int]("d")), const(3))))))
      )
    )
    QueryOptimizer.simplifyCondition(c) shouldEqual OrExpr(
      Seq(
        gt[Int](dimension(RawDimension[Int]("a")), dimension(RawDimension[Int]("b"))),
        gt[Int](dimension(RawDimension[Int]("b")), const(2)),
        gt[Int](dimension(RawDimension[Int]("c")), const(2)),
        gt[Int](dimension(RawDimension[Int]("d")), const(3))
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

  it should "handle OR in AND" in {
    val c = and(
      or(
        or(equ[Int](dimension(RawDimension[Int]("a")), const(1))),
        equ[Int](dimension(RawDimension[Int]("b")), const(2))
      ),
      or(
        or(
          equ[Int](dimension(RawDimension[Int]("c")), const(3)),
          equ[Int](dimension(RawDimension[Int]("d")), const(4))
        )
      )
    )

    val actual = QueryOptimizer.simplifyCondition(c)

    actual shouldEqual or(
      and(
        equ[Int](dimension(RawDimension[Int]("a")), const(1)),
        equ[Int](dimension(RawDimension[Int]("c")), const(3))
      ),
      and(
        equ[Int](dimension(RawDimension[Int]("a")), const(1)),
        equ[Int](dimension(RawDimension[Int]("d")), const(4))
      ),
      and(
        equ[Int](dimension(RawDimension[Int]("b")), const(2)),
        equ[Int](dimension(RawDimension[Int]("c")), const(3))
      ),
      and(
        equ[Int](dimension(RawDimension[Int]("b")), const(2)),
        equ[Int](dimension(RawDimension[Int]("d")), const(4))
      )
    )
  }

  "QueryOptimizer" should "optimize simple conditions" in {
    QueryOptimizer.optimizeExpr(calculator)(
      gt(dimension(TestDims.DIM_Y), plus(const(6L), const(36L)))
    ) shouldEqual gt(dimension(TestDims.DIM_Y), const(42L))
  }

  it should "optimize complex conditions" in {
    QueryOptimizer.optimizeExpr(calculator)(
      and(
        gt(dimension(TestDims.DIM_Y), plus(const(6L), const(36L))),
        lt(dimension(TestDims.DIM_Y), minus(const(100L), const(25L)))
      )
    ) shouldEqual and(gt(dimension(TestDims.DIM_Y), const(42L)), lt(dimension(TestDims.DIM_Y), const(75L)))
  }

  it should "optimize constant conditions" in {
    QueryOptimizer.optimizeExpr(calculator)(
      and(gt(const(5), const(2)), equ(const(1), const(1)))
    ) shouldEqual const(true)
  }

  it should "optimize if-then-else expressions" in {
    QueryOptimizer.optimizeExpr(calculator)(
      condition(equ(lower(dimension(TestDims.DIM_A)), lower(const("FOOooOO"))), const(1), plus(const(1), const(1)))
    ) shouldEqual condition(equ(lower(dimension(TestDims.DIM_A)), const("foooooo")), const(1), const(2))
  }

  it should "optimize inside aggregations" in {
    QueryOptimizer.optimizeExpr(calculator)(
      sum(
        condition(
          equ(lower(dimension(TestDims.DIM_A)), lower(const("AAAAAAAA"))),
          metric(TestTableFields.TEST_FIELD),
          minus(minus(const(2d), const(1d)), const(1d))
        )
      )
    ) shouldEqual sum(
      condition(
        equ(lower(dimension(TestDims.DIM_A)), const("aaaaaaaa")),
        metric(TestTableFields.TEST_FIELD),
        const(0d)
      )
    )
  }

  it should "optimize inside window functions" in {
    QueryOptimizer.optimizeExpr(calculator)(
      lag(
        condition(
          equ(lower(dimension(TestDims.DIM_A)), lower(const("AAAAAAAA"))),
          metric(TestTableFields.TEST_FIELD),
          minus(minus(const(2d), const(1d)), const(1d))
        )
      )
    ) shouldEqual lag(
      condition(
        equ(lower(dimension(TestDims.DIM_A)), const("aaaaaaaa")),
        metric(TestTableFields.TEST_FIELD),
        const(0d)
      )
    )
  }
}
