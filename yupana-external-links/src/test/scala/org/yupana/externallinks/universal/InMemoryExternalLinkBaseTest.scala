package org.yupana.externallinks.universal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ AddCondition, DataPoint, DimensionExpr, Expression, RemoveCondition }
import org.yupana.api.schema._
import org.yupana.core.ConstantCalculator
import org.yupana.core.model.{ BatchDataset, DatasetSchema }
import org.yupana.core.utils.FlatAndCondition
import org.yupana.externallinks.TestSchema
import org.yupana.utils.RussianTokenizer

import java.time.LocalDateTime

class InMemoryExternalLinkBaseTest extends AnyFlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  private val calculator = new ConstantCalculator(RussianTokenizer)

  class TestExternalLink(data: Array[Array[String]], override val externalLink: TestLink)
      extends InMemoryExternalLinkBase[TestLink](
        Seq(TestExternalLink.testField1, TestExternalLink.testField2, TestExternalLink.testField3),
        data
      ) {
    override val schema: Schema = TestSchema.schema

    val valueToKeys: Map[Int, Seq[String]] =
      Map(1 -> Seq("foo", "aaa"), 2 -> Seq("foo"), 3 -> Seq("bar"), 4 -> Seq("aaa"))

    override def keyIndex: Int = 0

    override def fillKeyValues(batch: BatchDataset): Unit = {
      batch.foreach { rowNum =>
        val tagValue = batch.get(rowNum, DimensionExpr(externalLink.dimension))
        val keyValue = valueToKeys.get(tagValue).flatMap(_.headOption)
        keyValue match {
          case Some(k) => batch.set(rowNum, keyExpr, k)
          case None    => batch.setNull(rowNum, keyExpr)
        }
      }
    }

    override def keyExpr: Expression[String] = dimension(DictionaryDimension("TAG_X"))

    override def put(dataPoints: Seq[DataPoint]): Unit = {}

    override def put(batchDataset: BatchDataset): Unit = {}
  }

  class TestLink extends ExternalLink {
    override type DimType = Int
    override val linkName: String = "TestCatalog"
    override val dimension: Dimension.Aux[Int] = RawDimension[Int]("TAG_Y")
    override val fields: Set[LinkField] =
      Set(TestExternalLink.testField1, TestExternalLink.testField2, TestExternalLink.testField3)
        .map(LinkField[String])
  }

  object TestExternalLink {
    val testField1 = "testField1"
    val testField2 = "testField2"
    val testField3 = "testField3"
  }

  val testData = Array(
    Array("foo", "bar", "baz"),
    Array("foo", "quux", "baz"),
    Array("bar", "bar", "look"),
    Array("aaa", "bbb", "at"),
    Array("qqq", "ccc", "me"),
    Array("foo", "ddd", "longest"),
    Array("bbb", "eee", "word")
  )

  val testExternalLink = new TestLink
  val testCatalog = new TestExternalLink(testData, testExternalLink)

  "InMemoryCatalogBase" should "fill value data" in {
    val valExprIndex = Map[Expression[_], Int](
      time -> 0,
      dimension(RawDimension[Int]("TAG_Y")) -> 1
    )
    val refExprIndex = Map[Expression[_], Int](
      dimension(DictionaryDimension("TAG_X")) -> 2,
      link(testExternalLink, TestExternalLink.testField1) -> 3,
      link(testExternalLink, TestExternalLink.testField2) -> 4,
      link(testExternalLink, TestExternalLink.testField3) -> 5
    )

    val schema = new DatasetSchema(valExprIndex, refExprIndex, Map.empty, None)
    val batch = new BatchDataset(schema)

    batch.set(0, time, Time(100))
    batch.set(0, dimension(RawDimension[Int]("TAG_Y")), 1)

    batch.set(1, time, Time(200))
    batch.set(1, dimension(RawDimension[Int]("TAG_Y")), 4)

    batch.set(2, time, Time(300))
    batch.set(2, dimension(RawDimension[Int]("TAG_Y")), 42)

    testCatalog.setLinkedValues(
      batch,
      Set(
        link(testExternalLink, TestExternalLink.testField1),
        link(testExternalLink, TestExternalLink.testField2),
        link(testExternalLink, TestExternalLink.testField3)
      )
    )

    batch.get[String](0, link(testExternalLink, TestExternalLink.testField1)) shouldEqual "foo"
    batch.get[String](0, link(testExternalLink, TestExternalLink.testField2)) shouldEqual "bar"
    batch.get[String](0, link(testExternalLink, TestExternalLink.testField3)) shouldEqual "baz"

    batch.get[String](1, link(testExternalLink, TestExternalLink.testField1)) shouldEqual "aaa"
    batch.get[String](1, link(testExternalLink, TestExternalLink.testField2)) shouldEqual "bbb"
    batch.get[String](1, link(testExternalLink, TestExternalLink.testField3)) shouldEqual "at"

    batch.isNull(2, link(testExternalLink, TestExternalLink.testField1)) shouldBe true
    batch.isNull(2, link(testExternalLink, TestExternalLink.testField2)) shouldBe true
    batch.isNull(2, link(testExternalLink, TestExternalLink.testField3)) shouldBe true

  }

  it should "support positive conditions" in {
    val c = equ(lower(link(testExternalLink, TestExternalLink.testField1)), const("aaa"))
    val t1 = LocalDateTime.of(2022, 10, 27, 1, 5)
    val t2 = t1.plusWeeks(1)
    testCatalog.transformCondition(
      FlatAndCondition(calculator, and(c, ge(time, const(Time(t1))), le(time, const(Time(t2))))).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c),
      AddCondition(in(lower(dimension(DictionaryDimension("TAG_X"))), Set("aaa")))
    )

    val c2 = equ(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar"))
    val c2_2 = equ(lower(link(testExternalLink, TestExternalLink.testField1)), const("bar"))
    testCatalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(t1))),
          le(time, const(Time(t2))),
          c2,
          c2_2
        )
      ).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c2),
      RemoveCondition(c2_2),
      AddCondition(in(lower(dimension(DictionaryDimension("TAG_X"))), Set("bar")))
    )

    val c3 = equ(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar"))
    val c3_2 = in(lower(link(testExternalLink, TestExternalLink.testField3)), Set("abc"))
    testCatalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          c3,
          c3_2,
          ge(time, const(Time(t1))),
          le(time, const(Time(t2)))
        )
      ).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c3),
      RemoveCondition(c3_2),
      AddCondition(in(lower(dimension(DictionaryDimension("TAG_X"))), Set.empty))
    )
  }

  it should "support negativeCondition operation" in {
    val c = neq(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar"))
    val t2 = LocalDateTime.now()
    val t1 = t2.minusDays(3)
    testCatalog.transformCondition(
      FlatAndCondition(calculator, and(ge(time, const(Time(t1))), le(time, const(Time(t2))), c)).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c),
      AddCondition(notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("foo", "bar")))
    )

    val c2 = neq(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar"))
    val c2_2 = notIn(lower(link(testExternalLink, TestExternalLink.testField3)), Set("look"))
    testCatalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(t1))),
          le(time, const(Time(t2))),
          c2,
          c2_2
        )
      ).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c2),
      RemoveCondition(c2_2),
      AddCondition(notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("foo", "bar")))
    )

    val c3 = neq(lower(link(testExternalLink, TestExternalLink.testField1)), const("aaa"))
    val c3_2 = neq(lower(link(testExternalLink, TestExternalLink.testField3)), const("baz"))
    testCatalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(t1))),
          le(time, const(Time(t2))),
          c3,
          c3_2
        )
      ).head
    ) should contain theSameElementsAs Seq(
      RemoveCondition(c3),
      RemoveCondition(c3_2),
      AddCondition(notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("aaa", "foo")))
    )
  }

  it should "validate data" in {
    testCatalog.validate()

    val invalidData = Array(
      Array("foo", "bar", "baz"),
      Array("foo", "quux"),
      Array("bar", "bar", "look")
    )

    val link = new TestExternalLink(invalidData, testExternalLink)
    the[IllegalArgumentException] thrownBy link.validate() should have message "Data must have exactly 3 columns"
  }
}
