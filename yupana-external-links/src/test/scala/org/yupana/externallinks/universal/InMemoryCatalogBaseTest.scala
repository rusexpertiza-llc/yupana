package org.yupana.externallinks.universal

import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ DimensionExpr, Expression }
import org.yupana.api.schema.{ DictionaryDimension, Dimension, ExternalLink, LinkField, RawDimension }
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }

class InMemoryCatalogBaseTest extends FlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  class TestExternalLink(data: Array[Array[String]], override val externalLink: TestLink)
      extends InMemoryExternalLinkBase[TestLink](
        Seq(TestExternalLink.testField1, TestExternalLink.testField2, TestExternalLink.testField3),
        data
      ) {
    val valueToKeys: Map[Int, Seq[String]] =
      Map(1 -> Seq("foo", "aaa"), 2 -> Seq("foo"), 3 -> Seq("bar"), 4 -> Seq("aaa"))

    override def keyIndex: Int = 0

    override def fillKeyValues(indexMap: collection.Map[Expression, Int], valueData: Seq[InternalRow]): Unit = {
      valueData.foreach { vd =>
        vd.get[Int](indexMap, DimensionExpr(externalLink.dimension)).foreach { tagValue =>
          val keyValue = valueToKeys.get(tagValue).flatMap(_.headOption)
          vd.set(indexMap, keyExpr, keyValue)
        }
      }
    }

    override def conditionForKeyValues(condition: Condition): Condition = {
      condition
    }

    override def keyExpr: Expression.Aux[String] = dimension(DictionaryDimension("TAG_X"))
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
    val exprIndex = Seq[Expression](
      time,
      dimension(RawDimension[Int]("TAG_Y")),
      link(testExternalLink, TestExternalLink.testField1),
      link(testExternalLink, TestExternalLink.testField2),
      link(testExternalLink, TestExternalLink.testField3)
    ).zipWithIndex.toMap

    val builder = new InternalRowBuilder(exprIndex, None)

    val valueData = Seq(
      builder.set(time, Some(Time(100))).set(dimension(RawDimension[Int]("TAG_Y")), Some(1)).buildAndReset(),
      builder.set(time, Some(Time(200))).set(dimension(RawDimension[Int]("TAG_Y")), Some(4)).buildAndReset(),
      builder.set(time, Some(Time(300))).set(dimension(RawDimension[Int]("TAG_Y")), Some(42)).buildAndReset()
    )

    testCatalog.setLinkedValues(
      exprIndex,
      valueData,
      Set(
        link(testExternalLink, TestExternalLink.testField1),
        link(testExternalLink, TestExternalLink.testField2),
        link(testExternalLink, TestExternalLink.testField3)
      )
    )

    val r1 = valueData(0)
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldEqual Some("foo")
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldEqual Some("bar")
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldEqual Some("baz")

    val r2 = valueData(1)
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldEqual Some("aaa")
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldEqual Some("bbb")
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldEqual Some("at")

    val r3 = valueData(2)
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldBe empty
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldBe empty
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldBe empty

  }

  it should "support positive conditions" in {
    testCatalog.condition(equ(lower(link(testExternalLink, TestExternalLink.testField1)), const("aaa"))) shouldEqual in(
      lower(dimension(DictionaryDimension("TAG_X"))),
      Set("aaa")
    )

    testCatalog.condition(
      and(
        equ(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar")),
        equ(lower(link(testExternalLink, TestExternalLink.testField1)), const("bar"))
      )
    ) shouldEqual in(lower(dimension(DictionaryDimension("TAG_X"))), Set("bar"))

    testCatalog.condition(
      and(
        equ(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar")),
        in(lower(link(testExternalLink, TestExternalLink.testField3)), Set("abc"))
      )
    ) shouldEqual in(lower(dimension(DictionaryDimension("TAG_X"))), Set.empty)
  }

  it should "support negativeCondition operation" in {
    testCatalog.condition(
      neq(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar"))
    ) shouldEqual notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("foo", "bar"))
    testCatalog.condition(
      and(
        neq(lower(link(testExternalLink, TestExternalLink.testField2)), const("bar")),
        notIn(lower(link(testExternalLink, TestExternalLink.testField3)), Set("look"))
      )
    ) shouldEqual notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("foo", "bar"))
    testCatalog.condition(
      and(
        neq(lower(link(testExternalLink, TestExternalLink.testField1)), const("aaa")),
        neq(lower(link(testExternalLink, TestExternalLink.testField3)), const("baz"))
      )
    ) shouldEqual notIn(lower(dimension(DictionaryDimension("TAG_X"))), Set("aaa", "foo"))
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
