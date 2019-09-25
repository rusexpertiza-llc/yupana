package org.yupana.hbase

import org.scalatest.{ FlatSpec, Inside, Matchers }
import org.yupana.api.schema._
import org.yupana.hbase.proto.SchemaRegistry

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
    override val fieldsNames: Set[String] = Set("foo", "bar")
  }

  val tables = Seq(table1, table2)

  val actualSchema = new SchemaRegistry(tables.map(ProtobufSchemaChecker.asProto))

  val schema = Schema(Seq(table1, table2), Seq.empty)

  "ProtobufSchemaChecker" should "successfully validate itself against expected TSDB schema" in {
    val expectedSchema = actualSchema.toByteArray
    ProtobufSchemaChecker.check(schema, expectedSchema) shouldBe Success
  }

  it should "return tables size diff warning" in {
    val mutatedSchemas = Seq(table1)
    val expectedSchema = new SchemaRegistry(mutatedSchemas.map(ProtobufSchemaChecker.asProto))
    ProtobufSchemaChecker.check(schema, expectedSchema.toByteArray) shouldBe Warning(
      "1 tables expected, but 2 actually present in " +
        "registry\nUnknown table table_2"
    )
  }

  it should "return check errors" in {
    val significantlyDifferentTable1 = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 31L,
      dimensionSeq = Seq(TAG_A, TAG_C),
      metrics = table1.metrics :+ Metric[BigDecimal]("extra_metric", 8),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedSchemas = Seq(significantlyDifferentTable1, table2)
    val expectedSchema = new SchemaRegistry(mutatedSchemas.map(ProtobufSchemaChecker.asProto))
    inside(ProtobufSchemaChecker.check(schema, expectedSchema.toByteArray)) {
      case Error(msg) =>
        msg shouldEqual "Expected rowTimeSpan for table table_1: 2678400000, actual: 2592000000\n" +
          "Expected dimensions for table table_1: tag_a, tag_c; actual: tag_b, tag_a, tag_c, tag_d\n" +
          "In table table_1 metric extra_metric has been removed"
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

    val mutatedSchemas = Seq(table1WithChangedGroups, table2)
    val expectedSchema = new SchemaRegistry(mutatedSchemas.map(ProtobufSchemaChecker.asProto))
    inside(ProtobufSchemaChecker.check(schema, expectedSchema.toByteArray)) {
      case Error(msg) =>
        msg shouldBe
          """In table table_1 metric metric_b has been removed
          |In table table_1 metric metric_b is unknown (new)""".stripMargin
    }
  }

  it should "return unknown field warning" in {
    val slightlyDifferentTable1 = new Table(
      name = "table_1",
      rowTimeSpan = 86400000L * 30L,
      dimensionSeq = Seq(TAG_B, TAG_A, TAG_C, TAG_D),
      metrics = table1.metrics.take(2),
      externalLinks = Seq(TEST_LINK)
    )

    val mutatedSchemas = Seq(slightlyDifferentTable1, table2)
    val expectedSchema = new SchemaRegistry(mutatedSchemas.map(ProtobufSchemaChecker.asProto))
    inside(ProtobufSchemaChecker.check(schema, expectedSchema.toByteArray)) {
      case Warning(msg) =>
        msg shouldBe
          """In table table_1 metric metric_c is unknown (new)
        |In table table_1 metric metric_d is unknown (new)""".stripMargin
    }
  }
}
