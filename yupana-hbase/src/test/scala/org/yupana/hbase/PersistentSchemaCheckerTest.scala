package org.yupana.hbase

import org.scalatest.Inside
import org.yupana.api.schema._
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ LocalDateTime, ZoneOffset }

class PersistentSchemaCheckerTest extends AnyFlatSpec with Matchers with Inside {

  val DIM_A = DictionaryDimension("dim_a")
  val DIM_B = RawDimension[Int]("dim_b")
  val DIM_C = RawDimension[Long]("dim_c")
  val DIM_D = DictionaryDimension("dim_d")

  val METRIC_A = Metric[Double]("metric_a", 1)
  val METRIC_B = Metric[Long]("metric_b", 2)
  val METRIC_C = Metric[String]("metric_c", 3)
  val METRIC_D = Metric[BigDecimal]("metric_d", 4)

  val metrics = Seq(METRIC_A, METRIC_B, METRIC_C, METRIC_D)

  val table1 = new Table(
    1,
    name = "table_1",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(DIM_B, DIM_A, DIM_C, DIM_D),
    metrics = metrics,
    externalLinks = Seq.empty,
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val table2 = new Table(
    1,
    name = "table_2",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(DIM_A, DIM_B, DIM_C, DIM_D),
    metrics = metrics,
    externalLinks = Seq.empty,
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val TEST_LINK = new ExternalLink {
    override type DimType = Int
    override val linkName: String = "test_link"
    override val dimension: Dimension.Aux[Int] = DIM_B
    override val fields: Set[LinkField] = Set("foo", "bar").map(LinkField[String])
  }

  val tables = Seq(table1, table2)

  val expectedSchema = Schema(tables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
  val expectedSchemaBytes = PersistentSchemaChecker.toBytes(expectedSchema)

  "ProtobufSchemaChecker" should "successfully validate schema against itself" in {
    PersistentSchemaChecker.check(expectedSchema, expectedSchemaBytes) shouldBe Success
  }

  it should "return tables size diff warning" in {
    val mutatedTables = Seq(table1)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes) shouldBe Warning(
      "2 tables expected, but 1 actually present in registry"
    )
  }

  it should "return check errors" in {
    val significantlyDifferentTable1 = new Table(
      1,
      name = "table_1",
      rowTimeSpan = 86400000L * 31L,
      dimensionSeq = Seq(DIM_A, DIM_C),
      metrics = table1.metrics.take(2),
      externalLinks = Seq(TEST_LINK),
      LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    )

    val mutatedTables = Seq(significantlyDifferentTable1, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    inside(PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldEqual "Expected rowTimeSpan for table table_1: 2592000000, actual: 2678400000\n" +
          "Expected dimensions for table table_1: dim_b, dim_a, dim_c, dim_d; actual: dim_a, dim_c\n" +
          "In table table_1 metric metric_c:VARCHAR has been removed or updated\n" +
          "In table table_1 metric metric_d:DECIMAL has been removed or updated"
    }
  }

  it should "return check errors when change group for a field" in {
    val METRIC_B_LOW_PRIORITY = Metric[Long]("metric_b", 2, 2)

    val table1WithChangedGroups = new Table(
      1,
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(DIM_B, DIM_A, DIM_C, DIM_D),
      metrics = Seq(METRIC_A, METRIC_B_LOW_PRIORITY, METRIC_C, METRIC_D),
      externalLinks = Seq(TEST_LINK),
      LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    )

    val mutatedTables = Seq(table1WithChangedGroups, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    inside(PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          """In table table_1 metric metric_b:BIGINT has been removed or updated
          |In table table_1 metric metric_b:BIGINT is unknown (new)""".stripMargin
    }
  }

  it should "return check errors when change tag for a field" in {
    val METRIC_B_WRONG_TAG = Metric[Long]("metric_b", 20, 2)

    val table1WithChangedTag = new Table(
      1,
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(DIM_B, DIM_A, DIM_C, DIM_D),
      metrics = Seq(METRIC_A, METRIC_B_WRONG_TAG, METRIC_C, METRIC_D),
      externalLinks = Seq(TEST_LINK),
      LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    )

    val mutatedTables = Seq(table1WithChangedTag, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    inside(PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          """In table table_1 metric metric_b:BIGINT has been removed or updated
            |In table table_1 metric metric_b:BIGINT is unknown (new)""".stripMargin
    }
  }

  it should "return unknown field warning" in {
    val slightlyDifferentTable1 = new Table(
      1,
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(DIM_B, DIM_A, DIM_C, DIM_D),
      metrics = table1.metrics :+ Metric[BigDecimal]("extra_metric", 8),
      externalLinks = Seq(TEST_LINK),
      LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    )

    val mutatedTables = Seq(slightlyDifferentTable1, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    inside(PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Warning(msg) =>
        msg shouldBe
          "In table table_1 metric extra_metric:DECIMAL is unknown (new)".stripMargin
    }
  }

  it should "returTSDaoHBaseTest.scala:4:25n check errors when a table has duplicated tags" in {
    val NEW_METRIC = Metric[Long]("new_metric", 2, 2)

    val table1WithNewMetric = new Table(
      1,
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(DIM_B, DIM_A, DIM_C, DIM_D),
      metrics = Seq(METRIC_A, METRIC_B, METRIC_C, METRIC_D, NEW_METRIC),
      externalLinks = Seq(TEST_LINK),
      LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    )

    val mutatedTables = Seq(table1WithNewMetric, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
    inside(PersistentSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          "In table table_1 2 metrics (metric_b, new_metric) share the same tag: 2\n" +
          "In table table_1 metric new_metric:BIGINT is unknown (new)"
    }
  }
}
