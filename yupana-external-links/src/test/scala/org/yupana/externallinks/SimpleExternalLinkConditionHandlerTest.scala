package org.yupana.externallinks

import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.{ Condition, Expression, LinkExpr }
import org.yupana.api.schema.{ Dimension, ExternalLink }
import org.yupana.core.model.InternalRow
import org.yupana.schema.externallinks.ItemsInvertedIndex

class SimpleExternalLinkConditionHandlerTest extends FlatSpec with Matchers {

  import org.yupana.api.query.syntax.All._

  class TestExternalLink(override val externalLink: TestLink) extends SimpleExternalLinkConditionHandler[TestLink] {
    override def includeCondition(values: Seq[(String, Set[String])]): Condition = {
      Condition.and(values.map {
        case (field, vs) => in(dimension(externalLink.dimension), vs.map(v => field + "_" + v))
      })
    }

    override def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
      Condition.and(values.map {
        case (field, vs) => notIn(dimension(externalLink.dimension), vs.map(v => field + "_" + v))
      })
    }

    override def setLinkedValues(
        exprIndex: collection.Map[Expression, Int],
        valueData: Seq[InternalRow],
        exprs: Set[LinkExpr]
    ): Unit = {}
  }

  val xDim = Dimension("TAG_X")
  val yDim = Dimension("tag_y")

  class TestLink extends ExternalLink {

    val field1 = "field1"
    val field2 = "field2"
    val field3 = "field3"

    override val linkName: String = "Test"
    override val dimension: Dimension = xDim
    override val fieldsNames: Set[String] = Set(field1, field2, field3)
  }

  val testExternalLink = new TestLink
  val testCatalog = new TestExternalLink(testExternalLink)

  "SimpleCatalogConditionHandler" should "support ==" in {
    testCatalog.condition(
      and(
        gt(time, const(Time(1000))),
        lt(time, const(Time(2000))),
        equ(link(testExternalLink, testExternalLink.field1), const("foo"))
      )
    ) shouldEqual and(
      ge(time, const(Time(1001))),
      lt(time, const(Time(2000))),
      in(dimension(xDim), Set("field1_foo"))
    )
  }

  it should "support IN" in {
    testCatalog.condition(
      and(
        equ(link(testExternalLink, testExternalLink.field2), const("bar")),
        in(link(testExternalLink, testExternalLink.field3), Set("aaa", "bbb")),
        neq(dimension(yDim), const("baz"))
      )
    ) shouldEqual and(
      in(dimension(xDim), Set("field2_bar")),
      in(dimension(xDim), Set("field3_aaa", "field3_bbb")),
      neq(dimension(yDim), const("baz"))
    )
  }

  it should "support !=" in {
    testCatalog.condition(
      and(
        ge(time, const(Time(1000))),
        lt(time, const(Time(2000))),
        neq(link(testExternalLink, testExternalLink.field1), const("foo"))
      )
    ) shouldEqual and(
      ge(time, const(Time(1000))),
      lt(time, const(Time(2000))),
      notIn(dimension(xDim), Set("field1_foo"))
    )
  }

  it should "support NOT IN" in {
    testCatalog.condition(
      and(
        in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("12345", "67890")),
        notIn(link(testExternalLink, testExternalLink.field1), Set("aaa", "bbb")),
        neq(dimension(yDim), const("baz"))
      )
    ) shouldEqual and(
      in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("12345", "67890")),
      notIn(dimension(xDim), Set("field1_aaa", "field1_bbb")),
      neq(dimension(yDim), const("baz"))
    )
  }
}
