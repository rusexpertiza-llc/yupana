/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core

import org.yupana.api.Time

import java.nio.charset.StandardCharsets
import java.util.UUID
import org.yupana.api.schema._
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }

import java.time.{ LocalDateTime, ZoneOffset }

object TestDims {
  val DIM_A = HashDimension(
    "A",
    (s: String) => (s.hashCode, UUID.nameUUIDFromBytes(s.getBytes(StandardCharsets.UTF_8)).getMostSignificantBits)
  )
  val DIM_B = RawDimension[Short]("B")
  val DIM_X = DictionaryDimension("X")
  val DIM_Y = RawDimension[Long]("Y")
}

object TestTableFields {
  val TEST_FIELD: Metric.Aux[Double] = Metric[Double]("testField", 1)
  val TEST_STRING_FIELD: Metric.Aux[String] = Metric[String]("testStringField", 2)
  val TEST_FIELD2: Metric.Aux[Double] = Metric[Double]("testField2", 3, 2)
  val TEST_LONG_FIELD: Metric.Aux[Long] = Metric[Long]("testLongField", 4, 2)
  val TEST_BIGDECIMAL_FIELD: Metric.Aux[BigDecimal] = Metric[BigDecimal]("testBigDecimalField", 5)
  val TEST_TIME_FIELD: Metric.Aux[Time] = Metric[Time]("testTimeField", 6)
  val TEST_BYTE_FIELD: Metric.Aux[Byte] = Metric[Byte]("testByteField", 7)
}

object TestTable2Fields {
  val TEST_FIELD: Metric.Aux[BigDecimal] = Metric[BigDecimal]("testField", 1)
  val TEST_FIELD2: Metric.Aux[Double] = Metric[Double]("testField2", 2)
  val TEST_FIELD3: Metric.Aux[BigDecimal] = Metric[BigDecimal]("testField3", 3)
  val TEST_FIELD4: Metric.Aux[Int] = Metric[Int]("testField4", 3)
}

object TestLinks {
  class TestLink extends ExternalLink {

    override type DimType = String
    override val linkName: String = "TestLink"
    override val dimension: Dimension.Aux[String] = TestDims.DIM_A
    override val fields: Set[LinkField] = Set("testField").map(LinkField[String])
  }

  val TEST_LINK: TestLink = new TestLink

  class TestLink2 extends ExternalLink {
    override type DimType = String
    override val linkName: String = "TestLink2"
    override val dimension: Dimension.Aux[String] = TestDims.DIM_A
    override val fields: Set[LinkField] = Set("testField2").map(LinkField[String])
  }

  val TEST_LINK2: TestLink2 = new TestLink2

  class TestLink3 extends ExternalLink {
    override type DimType = String
    override val linkName: String = "TestLink3"
    override val dimension: Dimension.Aux[String] = TestDims.DIM_A
    override val fields: Set[LinkField] =
      Set("testField3_1", "testField3_2", "testField3_3").map(LinkField[String])
  }

  val TEST_LINK3: TestLink3 = new TestLink3

  class TestLink4 extends ExternalLink {
    override type DimType = Short
    override val linkName: String = "TestLink4"
    override val dimension: Dimension.Aux[Short] = TestDims.DIM_B
    override val fields: Set[LinkField] = Set("testField4").map(LinkField[String])
  }

  val TEST_LINK4: TestLink4 = new TestLink4

  class TestLink5 extends ExternalLink {
    override type DimType = Short
    override val linkName: String = "TestLink5"
    override val dimension: Dimension.Aux[Short] = TestDims.DIM_B
    override val fields: Set[LinkField] = Set(LinkField[Double]("testField5D"), LinkField[String]("testField5S"))
  }

  val TEST_LINK5: TestLink5 = new TestLink5
}

object TestSchema {

  val testTable = new Table(
    id = 1,
    name = "test_table",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(TestDims.DIM_A, TestDims.DIM_B),
    metrics = Seq(
      TestTableFields.TEST_FIELD,
      TestTableFields.TEST_STRING_FIELD,
      TestTableFields.TEST_FIELD2,
      TestTableFields.TEST_LONG_FIELD,
      TestTableFields.TEST_BIGDECIMAL_FIELD,
      TestTableFields.TEST_TIME_FIELD,
      TestTableFields.TEST_BYTE_FIELD
    ),
    externalLinks =
      Seq(TestLinks.TEST_LINK, TestLinks.TEST_LINK2, TestLinks.TEST_LINK3, TestLinks.TEST_LINK4, TestLinks.TEST_LINK5),
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val testTable2 = new Table(
    id = 2,
    name = "test_table_2",
    rowTimeSpan = 7 * 24 * 3600 * 1000,
    dimensionSeq = Seq(TestDims.DIM_X, TestDims.DIM_Y),
    metrics = Seq(
      TestTable2Fields.TEST_FIELD,
      TestTable2Fields.TEST_FIELD2,
      TestTable2Fields.TEST_FIELD3,
      TestTable2Fields.TEST_FIELD4
    ),
    externalLinks = Seq(),
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val testTable3 = new Table(
    id = 3,
    name = "test_table_3",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(TestDims.DIM_A, TestDims.DIM_B, TestDims.DIM_X),
    metrics = Seq(
      TestTableFields.TEST_FIELD,
      TestTableFields.TEST_STRING_FIELD,
      TestTableFields.TEST_FIELD2,
      TestTableFields.TEST_LONG_FIELD,
      TestTableFields.TEST_BIGDECIMAL_FIELD
    ),
    externalLinks =
      Seq(TestLinks.TEST_LINK, TestLinks.TEST_LINK2, TestLinks.TEST_LINK3, TestLinks.TEST_LINK4, TestLinks.TEST_LINK5),
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val testTable4 = new Table(
    id = 4,
    name = "test_table_4",
    rowTimeSpan = 7 * 24 * 3600 * 1000,
    dimensionSeq = Seq(TestDims.DIM_X, TestDims.DIM_Y, TestDims.DIM_B),
    metrics = Seq(TestTable2Fields.TEST_FIELD, TestTable2Fields.TEST_FIELD2, TestTable2Fields.TEST_FIELD3),
    externalLinks = Seq(TestLinks.TEST_LINK4),
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val schema =
    Schema(Seq(testTable, testTable2, testTable4), Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
}
