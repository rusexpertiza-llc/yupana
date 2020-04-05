package org.yupana.core

import org.yupana.api.schema._

object TestDims {
  val TAG_A = Dimension("TAG_A")
  val TAG_B = Dimension("TAG_B")
  val TAG_X = Dimension("TAG_X")
  val TAG_Y = Dimension("TAG_Y")
}

object TestTableFields {
  val TEST_FIELD: Metric.Aux[Double] = Metric[Double]("testField", 1)
  val TEST_STRING_FIELD: Metric.Aux[String] = Metric[String]("testStringField", 2)
  val TEST_FIELD2: Metric.Aux[Double] = Metric[Double]("testField2", 3)
  val TEST_LONG_FIELD: Metric.Aux[Long] = Metric[Long]("testLongField", 4)
}

object TestTable2Fields {
  val TEST_FIELD: Metric.Aux[BigDecimal] = Metric[BigDecimal]("testField", 1)
  val TEST_FIELD2: Metric.Aux[Double] = Metric[Double]("testField2", 2)
  val TEST_FIELD3: Metric.Aux[BigDecimal] = Metric[BigDecimal]("testField3", 3)
}

object TestLinks {
  class TestLink extends ExternalLink {
    override val linkName: String = "TestLink"
    override val dimension: Dimension = TestDims.TAG_A
    override val fieldsNames: Set[String] = Set("testField")
  }

  val TEST_LINK: TestLink = new TestLink

  class TestLink2 extends ExternalLink {
    override val linkName: String = "TestLink2"
    override val dimension: Dimension = TestDims.TAG_A
    override val fieldsNames: Set[String] = Set("testField2")
  }

  val TEST_LINK2: TestLink2 = new TestLink2

  class TestLink3 extends ExternalLink {
    override val linkName: String = "TestLink3"
    override val dimension: Dimension = TestDims.TAG_A
    override val fieldsNames: Set[String] = Set("testField3_1", "testField3_2", "testField3_3")
  }

  val TEST_LINK3: TestLink3 = new TestLink3

  class TestLink4 extends ExternalLink {
    override val linkName: String = "TestLink4"
    override val dimension: Dimension = TestDims.TAG_B
    override val fieldsNames: Set[String] = Set("testField4")
  }

  val TEST_LINK4: TestLink4 = new TestLink4
}

object TestSchema {

  val testTable = new Table(
    name = "test_table",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(TestDims.TAG_A, TestDims.TAG_B),
    metrics = Seq(
      TestTableFields.TEST_FIELD,
      TestTableFields.TEST_STRING_FIELD,
      TestTableFields.TEST_FIELD2,
      TestTableFields.TEST_LONG_FIELD
    ),
    externalLinks = Seq(TestLinks.TEST_LINK, TestLinks.TEST_LINK2, TestLinks.TEST_LINK3, TestLinks.TEST_LINK4)
  )

  val testTable2 = new Table(
    name = "test_table_2",
    rowTimeSpan = 7 * 24 * 3600 * 1000,
    dimensionSeq = Seq(TestDims.TAG_X, TestDims.TAG_Y),
    metrics = Seq(TestTable2Fields.TEST_FIELD, TestTable2Fields.TEST_FIELD2, TestTable2Fields.TEST_FIELD3),
    externalLinks = Seq()
  )

  val schema = Schema(Seq(testTable, testTable2), Seq.empty)
}
