package org.yupana.core.providers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ EitherValues, Inspectors, OptionValues }
import org.yupana.api.types.{ DataType, SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.core.TestSchema

class JdbcMetadataProviderTest extends AnyFlatSpec with Inspectors with Matchers with OptionValues with EitherValues {
  implicit val srw: StringReaderWriter = SimpleStringReaderWriter
  val metadataProvider = new JdbcMetadataProvider(TestSchema.schema, 1, 2, "1.2")

  "JdbcMetadataProvider" should "return None when unknown table description has been requested" in {
    metadataProvider.describeTable("unknown_talbe") shouldBe Left("Unknown table 'unknown_talbe'")
  }

  it should "list tables" in {
    val res = metadataProvider.listTables
    res.fieldNames should contain theSameElementsAs metadataProvider.tableFieldNames
    res.dataTypes should contain only DataType[String]

    var cols = Seq.empty[Seq[String]]
    while (res.next()) {
      cols = cols :+ metadataProvider.tableFieldNames.map(res.get[String])
    }

    cols should contain theSameElementsInOrderAs Seq(
      Seq(null, null, "test_table", "TABLE", null),
      Seq(null, null, "test_table_2", "TABLE", null),
      Seq(null, null, "test_table_4", "TABLE", null)
    )
  }

  it should "specify version" in {
    val res = metadataProvider.version
    res.fieldNames should contain theSameElementsInOrderAs List("MAJOR", "MINOR", "VERSION")
    res.next() shouldBe true
    res.get[Int]("MAJOR") shouldBe 1
    res.get[Int]("MINOR") shouldBe 2
    res.get[String]("VERSION") shouldBe "1.2"
  }

  it should "describe table by name" in {
    val res1 = metadataProvider.describeTable("test_table_4").value
    res1.fieldNames should contain theSameElementsAs metadataProvider.columnFieldNames

    forAtLeast(1, 1 to 8) { _ =>
      res1.next() shouldBe true
      res1.get[String]("COLUMN_NAME") shouldBe "time"
      res1.get[String]("TABLE_NAME") shouldBe "test_table_4"
      res1.get[Int]("DATA_TYPE") shouldBe 93
      res1.get[String]("TYPE_NAME") shouldBe "TIMESTAMP"
    }

    val res2 = metadataProvider.describeTable("test_table_4").value

    forAtLeast(1, 1 to 8) { _ =>
      res2.next() shouldBe true
      res2.get[String]("COLUMN_NAME") shouldBe "time"
      res2.get[String]("TABLE_NAME") shouldBe "test_table_4"
      res2.get[Int]("DATA_TYPE") shouldBe 93
      res2.get[String]("TYPE_NAME") shouldBe "TIMESTAMP"
    }

    val res3 = metadataProvider.describeTable("test_table_4").value

    forAtLeast(2, 1 to 8) { _ =>
      res3.next() shouldBe true
      res3.get[String]("TYPE_NAME").contains("VARCHAR")
      res3.get[String]("TABLE_NAME") shouldBe "test_table_4"
      res3.get[Int]("DATA_TYPE") shouldBe 12
      res3.get[String]("COLUMN_NAME") should (equal("X") or equal("TestLink4_testField4"))
    }

    val res4 = metadataProvider.describeTable("test_table_4").value
    forAtLeast(1, 1 to 8) { _ =>
      res4.next() shouldBe true
      res4.get[String]("COLUMN_NAME") shouldBe "Y"
      res4.get[String]("TABLE_NAME") shouldBe "test_table_4"
      res4.get[Int]("DATA_TYPE") shouldBe -5
      res4.get[String]("TYPE_NAME") shouldBe "BIGINT"
    }
  }

  it should "provide functions for type" in {
    val res1 = metadataProvider
      .listFunctions("VARCHAR")
      .value

    var fs1 = Seq.empty[String]
    while (res1.next()) {
      fs1 = fs1 :+ res1.get[String]("NAME")
    }

    fs1 should contain theSameElementsAs List(
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

    val res2 = metadataProvider
      .listFunctions("DOUBLE")
      .value

    var fs2 = Seq.empty[String]
    while (res2.next()) {
      fs2 = fs2 :+ res2.get[String]("NAME")
    }

    fs2 should contain theSameElementsAs List(
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

    val res3 = metadataProvider
      .listFunctions("ARRAY[INTEGER]")
      .value

    var fs3 = Seq.empty[String]
    while (res3.next()) {
      fs3 = fs3 :+ res3.get[String]("NAME")
    }

    fs3 should contain theSameElementsAs List(
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
