package org.yupana.akka

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalatest.{ FlatSpec, Matchers, OptionValues }
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
    val cols = metadataProvider.tableFieldNames.map(r.get[String])
    cols should contain theSameElementsInOrderAs Seq(
      null,
      null,
      "s1",
      "TABLE",
      null
    )
  }

  it should "describe table by name" in {
    val res = metadataProvider.describeTable("s1").right.toOption.value
    res.fieldNames should contain theSameElementsAs metadataProvider.columnFieldNames
    val r = res.iterator.toList
    r should have size 6

    val timeColDescription = r.find(_.get[String]("COLUMN_NAME").contains("time")).value
    timeColDescription.get[String]("TABLE_NAME") shouldBe "s1"
    timeColDescription.get[Int]("DATA_TYPE") shouldBe 93
    timeColDescription.get[String]("TYPE_NAME") shouldBe "TIMESTAMP"

    val stringColsDescriptions = r.filter(_.get[String]("TYPE_NAME").contains("VARCHAR"))
    stringColsDescriptions should have size 4
    stringColsDescriptions foreach { d =>
      d.get[String]("TABLE_NAME") shouldBe "s1"
      d.get[Int]("DATA_TYPE") shouldBe 12
    }
    stringColsDescriptions.map(_.get[String]("COLUMN_NAME")) should
      contain theSameElementsAs Seq("t1", "t2", "f2", "c1_f1")

    val longColDescription = r.find(_.get[String]("COLUMN_NAME").contains("f1")).value
    longColDescription.get[String]("TABLE_NAME") shouldBe "s1"
    longColDescription.get[Int]("DATA_TYPE") shouldBe -5
    longColDescription.get[String]("TYPE_NAME") shouldBe "BIGINT"
  }

}

object TS {

  class C1 extends ExternalLink {
    override type DimType = String
    override val linkName: String = "c1"
    override val dimension: Dimension.Aux[String] = DictionaryDimension("t1")
    override val fields: Set[LinkField] = Set("f1").map(LinkField[String])
  }

  val S1 = new Table(
    name = "s1",
    rowTimeSpan = 12,
    dimensionSeq = Seq(DictionaryDimension("t1"), DictionaryDimension("t2")),
    metrics = Seq(Metric[Long]("f1", 1), Metric[String]("f2", 2)),
    externalLinks = Seq(new C1),
    new LocalDateTime(2016, 1, 1, 0, 0).toDateTime(DateTimeZone.UTC).getMillis
  )

  val schema = Schema(Seq(S1), Seq.empty)
}
