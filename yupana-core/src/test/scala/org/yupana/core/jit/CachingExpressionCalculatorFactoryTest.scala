package org.yupana.core.jit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.query.syntax.All._
import org.yupana.cache.CacheFactory
import org.yupana.core.{ TestDims, TestSchema, TestTableFields }
import org.yupana.settings.Settings

import java.time.LocalDateTime
import java.util.Properties

class CachingExpressionCalculatorFactoryTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val tokenizer = TestSchema.schema.tokenizer

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "CachingExpressionCalculatorFactory" should "cache compiled calculators" in {
    val cond = gt(plus(dimension(TestDims.DIM_B), const(1.toShort)), const(42.toShort))
    val now = Time(LocalDateTime.now())

    val query = Query(
      TestSchema.testTable,
      const(Time(123456789)),
      const(Time(234567890)),
      Seq(
        dimension(TestDims.DIM_A) as "A",
        metric(TestTableFields.TEST_FIELD) as "F",
        time as "T"
      ),
      cond
    )

    val pf = lt(metric(TestTableFields.TEST_FIELD), const(10d))

    val c1 = CachingExpressionCalculatorFactory.makeCalculator(query, now, IndexedSeq.empty, Some(pf), tokenizer)
    val c2 = CachingExpressionCalculatorFactory.makeCalculator(query, now, IndexedSeq.empty, Some(pf), tokenizer)

    c1 eq c2 shouldBe true
  }
}
