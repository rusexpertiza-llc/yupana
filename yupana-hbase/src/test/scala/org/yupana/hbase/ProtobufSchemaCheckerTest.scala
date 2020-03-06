package org.yupana.hbase

import org.scalatest.{ FlatSpec, Inside, Matchers }
import org.yupana.api.schema._

class ProtobufSchemaCheckerTest extends FlatSpec with Matchers with Inside {

  val TAG_A = Dimension("tag_a")
  val TAG_B = Dimension("tag_b")
  val TAG_C = Dimension("tag_c")
  val TAG_D = Dimension("tag_d")

  val METRIC_A = Metric[Double]("metric_a", 1)
  val METRIC_B = Metric[Long]("metric_b", 2)
  val METRIC_C = Metric[String]("metric_c", 3)
  val METRIC_D = Metric[BigDecimal]("metric_d", 4)

  val metrics = Seq(METRIC_A, METRIC_B, METRIC_C, METRIC_D)

  val table1 = new Table(
    name = "table_1",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
    metrics = metrics,
    externalLinks = Seq.empty
  )

  val table2 = new Table(
    name = "table_2",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(TAG_A, TAG_B, TAG_C, TAG_D),
    metrics = metrics,
    externalLinks = Seq.empty
  )

  val TEST_LINK = new ExternalLink {
    override val linkName: String = "test_link"
    override val dimension: Dimension = TAG_B
    override val fieldsNames = Set("foo", "bar") map LinkMetric[String]
  }

  val tables = Seq(table1, table2)

  val expectedSchema = Schema(tables, Seq.empty)
  val expectedSchemaBytes = ProtobufSchemaChecker.toBytes(expectedSchema)

  "ProtobufSchemaChecker" should "successfully validate schema against itself" in {
    ProtobufSchemaChecker.check(expectedSchema, expectedSchemaBytes) shouldBe Success
  }

  it should "return tables size diff warning" in {
    val mutatedTables = Seq(table1)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes) shouldBe Warning(
      "2 tables expected, but 1 actually present in registry"
    )
  }

  it should "return check errors" in {
    val significantlyDifferentTable1 = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 31L,
      dimensionSeq = Seq(TAG_A, TAG_C),
      metrics = table1.metrics.take(2),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedTables = Seq(significantlyDifferentTable1, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    inside(ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldEqual "Expected rowTimeSpan for table table_1: 2592000000, actual: 2678400000\n" +
          "Expected dimensions for table table_1: tag_b, tag_a, tag_c, tag_d; actual: tag_a, tag_c\n" +
          "In table table_1 metric metric_c has been removed or updated\n" +
          "In table table_1 metric metric_d has been removed or updated"
    }
  }

  it should "return check errors when change group for a field" in {
    val METRIC_B_LOW_PRIORITY = Metric[Long]("metric_b", 2, 2)

    val table1WithChangedGroups = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
      metrics = Seq(METRIC_A, METRIC_B_LOW_PRIORITY, METRIC_C, METRIC_D),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedTables = Seq(table1WithChangedGroups, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    inside(ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          """In table table_1 metric metric_b has been removed or updated
          |In table table_1 metric metric_b is unknown (new)""".stripMargin
    }
  }

  it should "return check errors when change tag for a field" in {
    val METRIC_B_WRONG_TAG = Metric[Long]("metric_b", 20, 2)

    val table1WithChangedTag = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
      metrics = Seq(METRIC_A, METRIC_B_WRONG_TAG, METRIC_C, METRIC_D),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedTables = Seq(table1WithChangedTag, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    inside(ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          """In table table_1 metric metric_b has been removed or updated
            |In table table_1 metric metric_b is unknown (new)""".stripMargin
    }
  }

  it should "return unknown field warning" in {
    val slightlyDifferentTable1 = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
      metrics = table1.metrics :+ Metric[BigDecimal]("extra_metric", 8),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedTables = Seq(slightlyDifferentTable1, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    inside(ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Warning(msg) =>
        msg shouldBe
          "In table table_1 metric extra_metric is unknown (new)".stripMargin
    }
  }

  it should "return check errors when a table has duplicated tags" in {
    val NEW_METRIC = Metric[Long]("new_metric", 2, 2)

    val table1WithNewMetric = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
      metrics = Seq(METRIC_A, METRIC_B, METRIC_C, METRIC_D, NEW_METRIC),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedTables = Seq(table1WithNewMetric, table2)
    val actualSchema = Schema(mutatedTables, Seq.empty)
    inside(ProtobufSchemaChecker.check(actualSchema, expectedSchemaBytes)) {
      case Error(msg) =>
        msg shouldBe
          "In table table_1 2 metrics (metric_b, new_metric) share the same tag: 2\n" +
            "In table table_1 metric new_metric is unknown (new)"
    }
  }
}
