package org.yupana.core.providers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ EitherValues, OptionValues }
import org.yupana.api.types.{ DataType, SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.core.sql.FunctionRegistry
import org.yupana.core.TestSchema

class JdbcMetadataProviderTest extends AnyFlatSpec with Matchers with OptionValues with EitherValues {
  implicit val srw: StringReaderWriter = SimpleStringReaderWriter
  val functionRegistry = new FunctionRegistry()
  val metadataProvider = new JdbcMetadataProvider(TestSchema.schema, functionRegistry, 1, 2, "1.2")

  "JdbcMetadataProvider" should "return None when unknown table description has been requested" in {
    metadataProvider.describeTable("unknown_talbe") shouldBe Left("Unknown table 'unknown_talbe'")
  }

  it should "list tables" in {
    val res = metadataProvider.listTables
    res.fieldNames should contain theSameElementsAs metadataProvider.tableFieldNames
    res.dataTypes should contain only DataType[String]
    val r = res.toList
    val cols = r.map(row => metadataProvider.tableFieldNames.map(row.get[String]))
    cols should contain theSameElementsInOrderAs Seq(
      Seq(null, null, "test_table", "TABLE", null),
      Seq(null, null, "test_table_2", "TABLE", null),
      Seq(null, null, "test_table_4", "TABLE", null)
    )
  }

  it should "specify version" in {
    val res = metadataProvider.version
    res.fieldNames should contain theSameElementsInOrderAs List("MAJOR", "MINOR", "VERSION")
    val row = res.rows.next()
    res.rows shouldBe empty
    row should contain theSameElementsInOrderAs List(1, 2, "1.2")
  }

  it should "describe table by name" in {
    val res = metadataProvider.describeTable("test_table_4").value
    res.fieldNames should contain theSameElementsAs metadataProvider.columnFieldNames
    val r = res.toList
    r should have size 8

    val timeColDescription = r.find(_.get[String]("COLUMN_NAME").contains("time")).value
    timeColDescription.get[String]("TABLE_NAME") shouldBe "test_table_4"
    timeColDescription.get[Int]("DATA_TYPE") shouldBe 93
    timeColDescription.get[String]("TYPE_NAME") shouldBe "TIMESTAMP"

    val stringColsDescriptions = r.filter(_.get[String]("TYPE_NAME").contains("VARCHAR"))
    stringColsDescriptions should have size 2
    stringColsDescriptions foreach { d =>
      d.get[String]("TABLE_NAME") shouldBe "test_table_4"
      d.get[Int]("DATA_TYPE") shouldBe 12
    }
    stringColsDescriptions.map(_.get[String]("COLUMN_NAME")) should
      contain theSameElementsAs Seq("X", "TestLink4_testField4")

    val longColDescription = r.find(_.get[String]("COLUMN_NAME").contains("Y")).value
    longColDescription.get[String]("TABLE_NAME") shouldBe "test_table_4"
    longColDescription.get[Int]("DATA_TYPE") shouldBe -5
    longColDescription.get[String]("TYPE_NAME") shouldBe "BIGINT"
  }

  it should "provide functions for type" in {
    metadataProvider
      .listFunctions("VARCHAR")
      .value
      .toList
      .map(row => row.get[String]("NAME")) should contain theSameElementsAs List(
      "count",
      "distinct_count",
      "distinct_random",
      "is_not_null",
      "is_null",
      "lag",
      "max",
      "min",
      "length",
      "tokens",
      "split",
      "lower",
      "upper"
    )

    metadataProvider
      .listFunctions("DOUBLE")
      .value
      .toList
      .map(row => row.get[String]("NAME")) should contain theSameElementsAs List(
      "count",
      "distinct_count",
      "distinct_random",
      "is_not_null",
      "is_null",
      "lag",
      "max",
      "min",
      "sum",
      "abs",
      "avg",
      "-"
    )

    metadataProvider
      .listFunctions("ARRAY[INTEGER]")
      .value
      .toList
      .map(row => row.get[String]("NAME")) should contain theSameElementsAs List(
      "array_to_string",
      "count",
      "distinct_count",
      "distinct_random",
      "is_not_null",
      "is_null",
      "lag",
      "length"
    )

    metadataProvider.listFunctions("BUBBLE").left.value shouldEqual "Unknown type BUBBLE"
  }

}
