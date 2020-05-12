package org.yupana.core

import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.types.WindowOperation

class QueryOptimizerTest extends FlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  "QueryOptimizer" should "optimize simple conditions" in {
    QueryOptimizer.optimizeExpr(
      gt(dimension(TestDims.DIM_Y), plus(const(6L), const(36L)))
    ) shouldEqual gt(dimension(TestDims.DIM_Y), const(42L))
  }

  it should "optimize complex conditions" in {
    QueryOptimizer
      .optimizeExpr(
        and(
          gt(dimension(TestDims.DIM_Y), plus(const(6L), const(36L))),
          lt(dimension(TestDims.DIM_Y), minus(const(100L), const(25L)))
        )
      ) shouldEqual and(gt(dimension(TestDims.DIM_Y), const(42L)), lt(dimension(TestDims.DIM_Y), const(75L)))
  }

  it should "optimize constant conditions" in {
    QueryOptimizer.optimizeExpr(
      and(gt(const(5), const(2)), equ(const(1), const(1)))
    ) shouldEqual const(true)
  }

  it should "optimize if-then-else expressions" in {
    QueryOptimizer.optimizeExpr(
      condition(equ(lower(dimension(TestDims.DIM_A)), lower(const("FOOooOO"))), const(1), plus(const(1), const(1)))
    ) shouldEqual condition(equ(lower(dimension(TestDims.DIM_A)), const("foooooo")), const(1), const(2))
  }

  it should "optimize inside aggregations" in {
    QueryOptimizer.optimizeExpr(
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
    QueryOptimizer.optimizeExpr(
      windowFunction(
        WindowOperation.lag[Double],
        condition(
          equ(lower(dimension(TestDims.DIM_A)), lower(const("AAAAAAAA"))),
          metric(TestTableFields.TEST_FIELD),
          minus(minus(const(2d), const(1d)), const(1d))
        )
      ).aux
    ) shouldEqual windowFunction(
      WindowOperation.lag[Double],
      condition(
        equ(lower(dimension(TestDims.DIM_A)), const("aaaaaaaa")),
        metric(TestTableFields.TEST_FIELD),
        const(0d)
      )
    )
  }
}
