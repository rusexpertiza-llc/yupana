package org.yupana.externallinks

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers, OptionValues }
import org.yupana.api.Time
import org.yupana.api.query.Expression
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.{ DictionaryDimension, ExternalLink, LinkField, RawDimension }
import org.yupana.core.model.InternalRowBuilder
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.schema.externallinks.ItemsInvertedIndex

class ExternalLinkUtilsTest extends FlatSpec with Matchers with MockFactory with OptionValues {

  import org.yupana.api.query.syntax.All._

  private def condition(condition: Condition): Condition = {
    ExternalLinkUtils.transformConditionT[String](TestLink.linkName, condition, includeCondition, excludeCondition)
  }

  private def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    and(values.map {
      case (field, vs) => in[String](dimension(xDim), vs.map(v => field + "_" + v))
    }: _*)
  }

  private def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    and(values.map {
      case (field, vs) => notIn(dimension(xDim), vs.map(v => field + "_" + v))
    }: _*)
  }

  private val xDim = DictionaryDimension("X")
  private val yDim = RawDimension[Int]("Y")

  object TestLink extends ExternalLink {

    val field1 = "field1"
    val field2 = "field2"
    val field3 = "field3"

    override type DimType = String
    override val linkName: String = "Test"
    override val dimension: DictionaryDimension = xDim
    override val fields: Set[LinkField] = Set(field1, field2, field3).map(LinkField[String])

  }

  private val table = new org.yupana.api.schema.Table(
    "test",
    1000,
    Seq(xDim, yDim),
    Seq.empty,
    Seq(TestLink),
    new LocalDateTime(2016, 1, 1, 0, 0).toDateTime(DateTimeZone.UTC).getMillis
  )

  "ExternalLinkUtils" should "support == condition" in {
    condition(
      and(
        gt(time, const(Time(1000))),
        lt(time, const(Time(2000))),
        equ(lower(link(TestLink, TestLink.field1)), const("foo"))
      )
    ) shouldEqual and(
      ge(time, const(Time(1001))),
      lt(time, const(Time(2000))),
      in(dimension(xDim), Set("field1_foo"))
    )
  }

  it should "support IN condition" in {
    condition(
      and(
        equ(lower(link(TestLink, TestLink.field2)), const("bar")),
        in(lower(link(TestLink, TestLink.field3)), Set("aaa", "bbb")),
        neq(dimension(yDim), const(4))
      )
    ) shouldEqual and(
      in(dimension(xDim), Set("field2_bar")),
      in(dimension(xDim), Set("field3_aaa", "field3_bbb")),
      neq(dimension(yDim), const(4))
    )
  }

  it should "support != condition" in {
    condition(
      and(
        ge(time, const(Time(1000))),
        lt(time, const(Time(2000))),
        neq(lower(link(TestLink, TestLink.field1)), const("foo"))
      )
    ) shouldEqual and(
      ge(time, const(Time(1000))),
      lt(time, const(Time(2000))),
      notIn(dimension(xDim), Set("field1_foo"))
    )
  }

  it should "support NOT IN condition" in {
    condition(
      and(
        in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("12345", "67890")),
        notIn(lower(link(TestLink, TestLink.field1)), Set("aaa", "bbb")),
        neq(dimension(yDim), const(33))
      )
    ) shouldEqual and(
      in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("12345", "67890")),
      notIn(dimension(xDim), Set("field1_aaa", "field1_bbb")),
      neq(dimension(yDim), const(33))
    )
  }

  it should "fill internal rows" in {
    val testSetter = mockFunction[Set[String], Set[String], Table[String, String, String]]

    testSetter.expects(Set("field1", "field2"), Set("foo", "bar")).onCall {
      (fields: Set[String], dimValues: Set[String]) =>
        SparseTable[String, String, String](dimValues.map(dv => dv -> fields.map(f => f -> s"$f:$dv").toMap).toMap)
    }

    val exprIndex = Map[Expression, Int](
      time -> 0,
      dimension(xDim) -> 1,
      link(TestLink, TestLink.field1) -> 2,
      link(TestLink, TestLink.field2) -> 3
    )

    val ib = new InternalRowBuilder(exprIndex, Some(table))

    val row1 = ib
      .set(time, Some(Time(10L)))
      .set(dimension(xDim), Some("foo"))
      .buildAndReset()

    val row2 = ib
      .set(dimension(xDim), Some("bar"))
      .set(time, Some(Time(20L)))
      .buildAndReset()
    val rows = Seq(row1, row2)

    ExternalLinkUtils.setLinkedValues[String](
      TestLink,
      exprIndex,
      rows,
      Set(link(TestLink, TestLink.field1), link(TestLink, TestLink.field2)),
      testSetter
    )

    row1.get(exprIndex, link(TestLink, TestLink.field1)).value shouldEqual "field1:foo"
    row1.get(exprIndex, link(TestLink, TestLink.field2)).value shouldEqual "field2:foo"
    row2.get(exprIndex, link(TestLink, TestLink.field1)).value shouldEqual "field1:bar"
    row2.get(exprIndex, link(TestLink, TestLink.field2)).value shouldEqual "field2:bar"
  }

  it should "cross join multiple values" in {
    ExternalLinkUtils.crossJoinFieldValues(
      Seq(
        "foo" -> Set(1, 2, 3),
        "bar" -> Set(3, 4),
        "foo" -> Set(2, 3, 4, 5),
        "baz" -> Set(42)
      )
    ) shouldEqual Seq(
      Map("foo" -> 2, "bar" -> 3, "baz" -> 42),
      Map("foo" -> 2, "bar" -> 4, "baz" -> 42),
      Map("foo" -> 3, "bar" -> 3, "baz" -> 42),
      Map("foo" -> 3, "bar" -> 4, "baz" -> 42)
    )
  }
}
