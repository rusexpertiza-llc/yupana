package org.yupana.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query

class ExpressionCalculatorTest extends AnyFlatSpec with Matchers {
  import org.yupana.api.query.syntax.All._

  "ExpressionCalculator" should "evaluate" in {
    val query = Query(
      TestSchema.testTable,
      const(Time(123456789)),
      const(Time(234567890)),
      Seq(dimension(TestDims.DIM_A) as "A", metric(TestTableFields.TEST_FIELD) as "F", time as "T")
    )
    val qc = QueryContext(query, None)

    val tree = ExpressionCalculator.generateCalculator(qc)

    println(tree)

//    val calc = ExpressionCalculator.makeCalculator(qc)
  }
}
