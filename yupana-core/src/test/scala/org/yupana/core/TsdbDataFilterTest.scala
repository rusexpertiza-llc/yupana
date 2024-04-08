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

import java.util.Properties
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query.{ ConditionTransformation, Expression, LinkExpr }
import org.yupana.core.model.{ BatchDataset, InternalQuery }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.schema.LinkField
import org.yupana.cache.CacheFactory
import org.yupana.core.utils.{ FlatAndCondition, SparseTable }
import org.yupana.settings.Settings
import org.yupana.utils.RussianTokenizer

import java.time.{ LocalDateTime, OffsetDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class TsdbDataFilterTest
    extends AnyFlatSpec
    with Matchers
    with TsdbMocks
    with OptionValues
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  import org.yupana.api.query.syntax.All._

  private val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  implicit private val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  override def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  val from: OffsetDateTime = LocalDateTime.of(2017, 10, 15, 12, 57).atOffset(ZoneOffset.UTC)
  val to: OffsetDateTime = from.plusDays(1)
  private def timeBounds(from: OffsetDateTime = from, to: OffsetDateTime = to) =
    s" AND time >= TIMESTAMP '${from.format(format)}' AND time < TIMESTAMP '${to.format(format)}'"

  "TSDB" should "execute query with filter by values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, testField, A, B FROM test_table WHERE testField = 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(metric(TestTableFields.TEST_FIELD), const(1012d))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime + 100))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1013d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true
    res.get[Time]("time_time") shouldBe Time(pointTime)
    res.get[Double]("testField") shouldBe 1012d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2.toShort
  }

  it should "execute query with filter by values and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, abs(testField) AS abs_test_field, A, B FROM test_table " +
      "WHERE testField = 1012 AND B = 31" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(metric(TestTableFields.TEST_FIELD), const(1012d)),
            equ(dimension(TestDims.DIM_B), const(31.toShort))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)

        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 31.toShort)

        batch.set(1, time, Time(pointTime + 100))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1013d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 31.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true
    res.get[Time]("time_time") shouldBe Time(pointTime)
    res.get[Double]("abs_test_field") shouldBe 1012d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 31
  }

  it should "execute query with filter by values not presented in query.fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, A, B FROM test_table WHERE testField <= 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            le(metric(TestTableFields.TEST_FIELD), const(1012d))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)

        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime + 100))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1013d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)

        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("time_time") shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy res.get[Double]("testField")
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2
  }

  it should "execute query with filter by values comparing two ValueExprs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, A, B FROM test_table WHERE testField != testField2" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD2),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, metric(TestTableFields.TEST_FIELD2), 1013d)
        batch.set(0, dimension(TestDims.DIM_A), "test11")
        batch.set(0, dimension(TestDims.DIM_B), 1.toShort)

        batch.set(1, time, Time(pointTime + 100))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1013d)
        batch.set(1, metric(TestTableFields.TEST_FIELD2), 1013d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("time_time") shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy res.get[Double]("testField")
    res.get[String]("A") shouldBe "test11"
    res.get[Short]("B") shouldBe 1
  }

  it should "support IN for values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, A, B, testField as F1 FROM test_table WHERE F1 IN (1012, 1014)" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1014d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)

    res.next() shouldBe true
    res.get[Time]("time") shouldBe Time(pointTime)
    res.get[Double]("F1") shouldBe 1012d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2

    res.next() shouldBe true
    res.get[Time]("time") shouldBe Time(pointTime)
    res.get[Double]("F1") shouldBe 1014d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2

    res.next() shouldBe false
  }

  it should "support NOT IN for values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, A, B, testField as F1 FROM test_table WHERE F1 NOT IN (123, 456)" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(metric(TestTableFields.TEST_FIELD), Set(123d, 456d))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 123d)
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 234d)
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(2, time, Time(pointTime))
        batch.setNull(2, metric(TestTableFields.TEST_FIELD))
        batch.set(2, dimension(TestDims.DIM_A), "test1")
        batch.set(2, dimension(TestDims.DIM_B), 2.toShort)

        Iterator(batch)
      }

    val res = tsdb.query(query)

    res.next() shouldBe true

    res.get[Time]("time") shouldBe Time(pointTime)
    res.get[Double]("F1") shouldBe 234d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2

    res.next() shouldBe false
  }

  it should "support AND for values, catalogs and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql = "SELECT time, A, B, testField as F1 FROM test_table " +
      "WHERE F1 IN (1012, 1014) AND testStringField != 'Str@!' AND TestLink2_testField2 = 'Str@!Ster'" +
      timeBounds()
    val query = createQuery(sql)

    val c = equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("str@!ster"))
    (testCatalogServiceMock.transformCondition _)
      .expects(
        FlatAndCondition.single(
          calculator,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
            neq(lower(metric(TestTableFields.TEST_STRING_FIELD)), const("str@!")),
            equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("str@!ster"))
          )
        )
      )
      .returning(
        ConditionTransformation.replace(
          Seq(c),
          in(dimension(TestDims.DIM_A), Set("test1"))
        )
      )

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_STRING_FIELD),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
            neq(lower(metric(TestTableFields.TEST_STRING_FIELD)), const("str@!")),
            in(dimension(TestDims.DIM_A), Set("test1"))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(0, metric(TestTableFields.TEST_STRING_FIELD), "asdsadasd")
        batch.set(0, dimension(TestDims.DIM_A), "test1")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 1012d)
        batch.set(1, metric(TestTableFields.TEST_STRING_FIELD), "Str@!")
        batch.set(1, dimension(TestDims.DIM_A), "test1")
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(2, time, Time(pointTime))
        batch.set(2, metric(TestTableFields.TEST_FIELD), 1013d)
        batch.set(2, dimension(TestDims.DIM_A), "test1")
        batch.set(2, dimension(TestDims.DIM_B), 2.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("time") shouldBe Time(pointTime)
    res.get[Double]("F1") shouldBe 1012d
    res.get[String]("A") shouldBe "test1"
    res.get[Short]("B") shouldBe 2
    res.next() shouldBe false

  }

  it should "support IS NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT day(time) AS t, testField, A, B " +
      "FROM test_table WHERE TestLink_testField IS NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.transformCondition _)
      .expects(FlatAndCondition.single(calculator, condition))
      .returning(Seq.empty)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((dataset, _) =>
        setCatalogValueByTag(
          dataset,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          condition
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime1))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 10d)
        batch.set(0, dimension(TestDims.DIM_A), "test1a")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime1))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 30d)
        batch.set(1, dimension(TestDims.DIM_A), "test2a")
        batch.set(1, dimension(TestDims.DIM_B), 3.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
    res.get[Double]("testField") shouldBe 10d
    res.get[String]("A") shouldBe "test1a"
    res.get[Short]("B") shouldBe 2
    res.next() shouldBe false
  }

  it should "support IS NOT NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT hour(time) AS t, testField, A, B, TestLink_testField AS ctf " +
      "FROM test_table WHERE ctf IS NOT NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNotNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.transformCondition _)
      .expects(FlatAndCondition.single(calculator, condition))
      .returning(Seq.empty)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((ds, _) =>
        setCatalogValueByTag(
          ds,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          condition
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime1))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 10d)
        batch.set(0, dimension(TestDims.DIM_A), "test1a")
        batch.set(0, dimension(TestDims.DIM_B), 2.toShort)

        batch.set(1, time, Time(pointTime1))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 30d)
        batch.set(1, dimension(TestDims.DIM_A), "test2a")
        batch.set(1, dimension(TestDims.DIM_B), 3.toShort)
        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.HOURS).toInstant.toEpochMilli)
    res.get[Double]("testField") shouldBe 30d
    res.get[String]("A") shouldBe "test2a"
    res.get[Short]("B") shouldBe 3
    res.get[String]("ctf") shouldBe "some-value"

    res.next() shouldBe false
  }

  it should "support IS NULL and IS NOT NULL for catalog fields within AND among other conditions" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
      val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

      val sql = "SELECT day(time) AS t, testField, A, B, TestLink2_testField2 AS cf2 " +
        "FROM test_table " +
        "WHERE TestLink_testField IS NULL AND cf2 IS NOT NULL AND testField >= 1000 AND A != 'test1' AND B = 15" + timeBounds()
      val query = createQuery(sql)

      val cs = Seq(
        ge(time, const(Time(from))),
        lt(time, const(Time(to))),
        isNull(link(TestLinks.TEST_LINK, "testField")),
        isNotNull(link(TestLinks.TEST_LINK2, "testField2")),
        ge(metric(TestTableFields.TEST_FIELD), const(1000d)),
        neq(lower(dimension(TestDims.DIM_A)), const("test1")),
        equ(dimension(TestDims.DIM_B), const(15.toShort))
      )

      val condition = and(cs: _*)

      (testCatalogServiceMock.transformCondition _)
        .expects(FlatAndCondition.single(calculator, condition))
        .returning(Seq.empty)

      (testCatalogServiceMock2.transformCondition _)
        .expects(FlatAndCondition.single(calculator, condition))
        .returning(Seq.empty)

      (testCatalogServiceMock.setLinkedValues _)
        .expects(*, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
        .onCall((ds, _) =>
          setCatalogValueByTag(
            ds,
            TestLinks.TEST_LINK,
            SparseTable("test2a" -> Map("testField" -> "some-value"))
          )
        )

      (testCatalogServiceMock2.setLinkedValues _)
        .expects(*, Set(link(TestLinks.TEST_LINK2, "testField2")).asInstanceOf[Set[LinkExpr[_]]])
        .onCall((ds, _) =>
          setCatalogValueByTag(
            ds,
            TestLinks.TEST_LINK2,
            SparseTable("test1a" -> Map("testField2" -> "c2-value"), "test2a" -> Map("testField2" -> "some-value"))
          )
        )

      val pointTime1 = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set[Expression[_]](
              time,
              metric(TestTableFields.TEST_FIELD),
              dimension(TestDims.DIM_A),
              dimension(TestDims.DIM_B)
            ),
            condition
          ),
          *,
          *,
          *
        )
        .onCall { (_, _, dsSchema, _) =>
          val batch = new BatchDataset(dsSchema)
          batch.set(0, time, Time(pointTime1))
          batch.set(0, metric(TestTableFields.TEST_FIELD), 1001d)
          batch.set(0, dimension(TestDims.DIM_A), "test2a")
          batch.set(0, dimension(TestDims.DIM_B), 15.toShort)

          batch.set(1, time, Time(pointTime1 + 10))
          batch.set(1, metric(TestTableFields.TEST_FIELD), 1002d)
          batch.set(1, dimension(TestDims.DIM_A), "test2a")
          batch.set(1, dimension(TestDims.DIM_B), 15.toShort)

          batch.set(2, time, Time(pointTime1 + 10))
          batch.set(2, metric(TestTableFields.TEST_FIELD), 103d)
          batch.set(2, dimension(TestDims.DIM_A), "test2a")
          batch.set(2, dimension(TestDims.DIM_B), 15.toShort)

          batch.set(3, time, Time(pointTime1 + 10))
          batch.set(3, metric(TestTableFields.TEST_FIELD), 1003d)
          batch.set(3, dimension(TestDims.DIM_A), "test1a")
          batch.set(3, dimension(TestDims.DIM_B), 15.toShort)
          Iterator(batch)
        }

      val res = tsdb.query(query)
      res.next() shouldBe true

      res.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
      res.get[Double]("testField") shouldBe 1003d
      res.get[String]("A") shouldBe "test1a"
      res.get[Short]("B") shouldBe 15.toShort

      res.next() shouldBe false
  }

  it should "support IS NULL and IS NOT NULL inside CASE" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
    val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql =
      "SELECT day(time) AS d, sum(CASE WHEN TestLink_testField IS NOT NULL THEN testField ELSE 0) as quantity " +
        "FROM test_table " +
        "WHERE TestLink2_testField2 = 'test2'" + timeBounds() + " GROUP BY d"
    val query = createQuery(sql)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((ds, _) =>
        setCatalogValueByTag(
          ds,
          TestLinks.TEST_LINK,
          SparseTable("test1a" -> Map("testField" -> "c1-value"))
        )
      )

    val c = equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("test2"))
    (testCatalogServiceMock2.transformCondition _)
      .expects(
        FlatAndCondition.single(
          calculator,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            c
          )
        )
      )
      .returning(
        ConditionTransformation.replace(
          Seq(c),
          in(lower(dimension(TestDims.DIM_A)), Set("test1a", "test2a"))
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(lower(dimension(TestDims.DIM_A)), Set("test1a", "test2a"))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime1))
        batch.set(0, metric(TestTableFields.TEST_FIELD), 1011d)
        batch.set(0, dimension(TestDims.DIM_A), "test1a")

        batch.set(1, time, Time(pointTime2))
        batch.set(1, metric(TestTableFields.TEST_FIELD), 3001d)
        batch.set(1, dimension(TestDims.DIM_A), "test2a")

        Iterator(batch)
      }

    val res = tsdb.query(query)
    res.next() shouldBe true

    res.get[Time]("d") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
    res.get[Double]("quantity") shouldBe 1011d

    res.next() shouldBe false
  }

  it should "filter before calculation if possible" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, testField3 / testField2 as div FROM test_table_2 WHERE testField2 <> 0" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable2,
          Set[Expression[_]](time, metric(TestTable2Fields.TEST_FIELD2), metric(TestTable2Fields.TEST_FIELD3)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(metric(TestTable2Fields.TEST_FIELD2), const(0d))
          )
        ),
        *,
        *,
        *
      )
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, time, Time(pointTime))
        batch.set(0, metric(TestTable2Fields.TEST_FIELD2), 0d)
        batch.set(0, metric(TestTable2Fields.TEST_FIELD3), BigDecimal(5))
        Iterator(batch)
      }

    val results = tsdb.query(query)
    results.next() shouldBe false
  }

  it should "support numeric filtering on external links fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>

    val link5 = mockCatalogService(tsdb, TestLinks.TEST_LINK5)
    val sql = "SELECT B FROM test_table WHERE TestLink5_testField5D > 20" + timeBounds()
    val query = createQuery(sql)

    val doubleLinkExpr = link[Double](TestLinks.TEST_LINK5, LinkField[Double]("testField5D"))

    (tsdbDaoMock.query _)
      .expects(*, *, *, *)
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, Time(from.plusMinutes(10)))
        batch.set(0, dimension(TestDims.DIM_B), 12.toShort)
        batch.set(1, Time(from.plusHours(3)))
        batch.set(1, dimension(TestDims.DIM_B), 15.toShort)
        Iterator(batch)
      }

    (link5.setLinkedValues _)
      .expects(*, *)
      .onCall { (ds, _) =>
        for (i <- 0 until ds.size) {
          val v = ds.get(i, dimension(TestDims.DIM_B))
          ds.set(i, doubleLinkExpr, if (v == 12) 10.0 else 30.0)
        }
      }

    (link5.transformCondition _).expects(*).onCall((_: FlatAndCondition) => Seq.empty)

    val res = tsdb.query(query)

    res.next() shouldEqual true
    res.get[Int]("B") shouldBe 15
  }

  it should "support OR conditions" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT B, testField from test_table where (B IN (1,2,3) OR testField = 8)" + timeBounds()

    val query = createQuery(sql)

    (tsdbDaoMock.query _)
      .expects(*, *, *, *)
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, Time(from.plusMinutes(2)))
        batch.set(0, dimension(TestDims.DIM_B), 1.toShort)
        batch.set(0, metric(TestTableFields.TEST_FIELD), 4d)

        batch.set(1, Time(from.plusMinutes(2)))
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)
        batch.set(1, metric(TestTableFields.TEST_FIELD), 8d)
        Iterator(batch)
      }

    val rows = tsdb.query(query)

    rows.next() shouldBe true
    rows.next() shouldBe true
  }

  it should "support OR on different times" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, B, testField from test_table where (B IN (1,2,3)" + timeBounds() +
      ") OR (testField = 8" + timeBounds(from.minusYears(1), to.minusYears(1)) + ")"

    val query = createQuery(sql)

    (tsdbDaoMock.query _)
      .expects(*, *, *, *)
      .onCall { (_, _, dsSchema, _) =>
        val batch = new BatchDataset(dsSchema)
        batch.set(0, Time(from.plusMinutes(2)))
        batch.set(0, dimension(TestDims.DIM_B), 1.toShort)
        batch.set(0, metric(TestTableFields.TEST_FIELD), 4d)

        batch.set(1, Time(from.plusMinutes(2)))
        batch.set(1, dimension(TestDims.DIM_B), 2.toShort)
        batch.set(1, metric(TestTableFields.TEST_FIELD), 8d)

        batch.set(2, Time(from.minusYears(1).plusMinutes(2)))
        batch.set(2, dimension(TestDims.DIM_B), 1.toShort)
        batch.set(2, metric(TestTableFields.TEST_FIELD), 4d)

        batch.set(3, Time(from.minusYears(1).plusMinutes(2)))
        batch.set(3, dimension(TestDims.DIM_B), 2.toShort)
        batch.set(3, metric(TestTableFields.TEST_FIELD), 8d)
        Iterator(batch)
      }

    val rows = tsdb.query(query)

    var i = 0
    var fl = false
    while (rows.next()) {
      val t = rows.get[Time]("time") == Time(from.minusYears(1).plusMinutes(2)) && rows.get[Double]("testField") != 8d
      fl = fl || t
      i += 1
    }
    i shouldBe 3
    fl shouldBe false
  }
}
