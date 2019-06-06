package org.yupana.akka

import org.scalatest.{FlatSpec, Matchers, OptionValues}
import org.yupana.api.schema._
import org.yupana.api.types.DataType

class JdbcMetadataProviderTest extends FlatSpec with Matchers with OptionValues {

  val metadataProvider = new JdbcMetadataProvider(TS.schema)

  "JdbcMetadataProvider" should "return None when unknown table description has been requested" in {
    metadataProvider.describeTable("unknown_talbe") shouldBe Left("Unknown schema 'unknown_talbe'")
  }

  it should "list tables" in {
    val res = metadataProvider.listTables
    res.fieldNames should contain theSameElementsAs metadataProvider.tableFieldNames
    res.dataTypes should contain only DataType[String]
    val r = res.iterator.next
    val cols = metadataProvider.tableFieldNames.map(r.fieldValueByName[String])
    cols should contain theSameElementsInOrderAs Seq(
      None, None, Some("s1"), Some("TABLE"), None
    )
  }

  it should "describe table by name" in {
    val res = metadataProvider.describeTable("s1").right.toOption.value
    res.fieldNames should contain theSameElementsAs metadataProvider.columnFieldNames
    val r = res.iterator.toList
    r should have size 6

    val timeColDescription = r.find(_.fieldValueByName[String]("COLUMN_NAME").contains("time")).value
    timeColDescription.fieldValueByName[String]("TABLE_NAME").value shouldBe "s1"
    timeColDescription.fieldValueByName[Int]("DATA_TYPE").value shouldBe 93
    timeColDescription.fieldValueByName[String]("TYPE_NAME").value shouldBe "TIMESTAMP"

    val stringColsDescriptions = r.filter(_.fieldValueByName[String]("TYPE_NAME").contains("VARCHAR"))
    stringColsDescriptions should have size 4
    stringColsDescriptions foreach { d =>
      d.fieldValueByName[String]("TABLE_NAME").value shouldBe "s1"
      d.fieldValueByName[Int]("DATA_TYPE").value shouldBe 12
    }
    stringColsDescriptions.map(_.fieldValueByName("COLUMN_NAME")) should
      contain theSameElementsAs Seq("t1", "t2", "f2", "c1_f1").map(Option(_))

    val longColDescription = r.find(_.fieldValueByName[String]("COLUMN_NAME").contains("f1")).value
    longColDescription.fieldValueByName[String]("TABLE_NAME").value shouldBe "s1"
    longColDescription.fieldValueByName[Int]("DATA_TYPE").value shouldBe -5
    longColDescription.fieldValueByName[String]("TYPE_NAME").value shouldBe "BIGINT"
  }

}

object TS {

  class C1 extends ExternalLink {
    override val linkName: String = "c1"
    override val dimension: Dimension = Dimension("t1")
    override val fieldsNames: Set[String] = Set("f1")
  }

  val S1 = new Table(
    name = "s1",
    rowTimeSpan = 12,
    dimensionSeq = Seq(Dimension("t1"), Dimension("t2")),
    metrics = Seq(Metric[Long]("f1", 1), Metric[String]("f2", 2)),
    externalLinks = Seq(new C1)
  )

  val schema = Schema(Seq(S1), Seq.empty)
}
