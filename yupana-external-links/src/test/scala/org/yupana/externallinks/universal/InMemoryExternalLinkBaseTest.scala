package org.yupana.externallinks.universal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ AddCondition, DimensionExpr, Expression, RemoveCondition }
import org.yupana.api.schema._
import org.yupana.core.ConstantCalculator
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
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

    override def fillKeyValues(indexMap: collection.Map[Expression[_], Int], valueData: Seq[InternalRow]): Unit = {
      valueData.foreach { vd =>
        val tagValue = vd.get(indexMap, DimensionExpr(externalLink.dimension))
        val keyValue = valueToKeys.get(tagValue).flatMap(_.headOption).orNull
        vd.set(indexMap, keyExpr, keyValue)
      }
    }

    override def keyExpr: Expression[String] = dimension(DictionaryDimension("TAG_X"))
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
    val exprIndex = Seq[Expression[_]](
      time,
      dimension(RawDimension[Int]("TAG_Y")),
      link(testExternalLink, TestExternalLink.testField1),
      link(testExternalLink, TestExternalLink.testField2),
      link(testExternalLink, TestExternalLink.testField3)
    ).zipWithIndex.toMap

    val builder = new InternalRowBuilder(exprIndex, None)

    val valueData = Seq(
      builder.set(time, Time(100)).set(dimension(RawDimension[Int]("TAG_Y")), 1).buildAndReset(),
      builder.set(time, Time(200)).set(dimension(RawDimension[Int]("TAG_Y")), 4).buildAndReset(),
      builder.set(time, Time(300)).set(dimension(RawDimension[Int]("TAG_Y")), 42).buildAndReset()
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
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldEqual "foo"
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldEqual "bar"
    r1.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldEqual "baz"

    val r2 = valueData(1)
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldEqual "aaa"
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldEqual "bbb"
    r2.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldEqual "at"

    val r3 = valueData(2)
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField1)) shouldBe null
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField2)) shouldBe null
    r3.get[String](exprIndex, link(testExternalLink, TestExternalLink.testField3)) shouldBe null

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
