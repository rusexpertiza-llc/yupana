package org.yupana.externallinks.items

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ Query, Replace }
import org.yupana.core._
import org.yupana.core.utils.TimeBoundedCondition
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.externallinks.TestSchema
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }
import org.yupana.utils.RussianTokenizer

class RelatedItemsCatalogImplTest extends AnyFlatSpec with Matchers with MockFactory {
  import org.yupana.api.query.syntax.All._

  class MockedTsdb
      extends TSDB(TestSchema.schema, null, null, null, identity, SimpleTsdbConfig(), { _: Query => NoMetricCollector })

  private val calculator = new ConstantCalculator(RussianTokenizer)

  "RelatedItemsCatalogImpl" should "handle phrase field in conditions" in {
    val tsdb = mock[MockedTsdb]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery1 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("хлеб ржаной"))
    )

    val qc1 = new QueryContext(expQuery1, None, ExpressionCalculatorFactory)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery1)
      .returning(
        new TsdbServerResult(
          qc1,
          Seq(
            Array[Any](123456, Time(120)),
            Array[Any](123456, Time(150)),
            Array[Any](345112, Time(120))
          ).iterator
        )
      )

    val expQuery2 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("бородинский"))
    )

    val qc2 = new QueryContext(expQuery2, None, ExpressionCalculatorFactory)

    (tsdb.query _)
      .expects(expQuery2)
      .returning(
        new TsdbServerResult(
          qc2,
          Seq(
            Array[Any](123456, Time(125)),
            Array[Any](123456, Time(120))
          ).iterator
        )
      )

    val c1 = in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("хлеб ржаной"))
    val c2 = notIn(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("бородинский"))

    val conditions = catalog.transformCondition(
      TimeBoundedCondition(
        calculator,
        and(
          ge(time, const(Time(100L))),
          lt(time, const(Time(500L))),
          c1,
          c2
        )
      ).head
    )

    conditions shouldEqual Seq(
      Replace(
        Set(c1),
        in(
          tuple(time, dimension(Dimensions.KKM_ID)),
          Set((Time(120L), 123456), (Time(150L), 123456), (Time(120L), 345112))
        )
      ),
      Replace(
        Set(c2),
        notIn(
          tuple(time, dimension(Dimensions.KKM_ID)),
          Set((Time(125L), 123456), (Time(120L), 123456))
        )
      )
    )
  }

  it should "handle item field in conditions" in {
    val tsdb = mock[MockedTsdb]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(dimension(Dimensions.ITEM)), Set("яйцо молодильное 1к"))
    )

    val qc = new QueryContext(expQuery, None, ExpressionCalculatorFactory)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery)
      .returning(
        new TsdbServerResult(
          qc,
          Seq(
            Array[Any](123456, Time(220)),
            Array[Any](654321, Time(330))
          ).iterator
        )
      )

    val c = in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.ITEM_FIELD)), Set("яйцо молодильное 1к"))
    val conditions = catalog.transformCondition(
      TimeBoundedCondition(
        calculator,
        and(
          ge(time, const(Time(100L))),
          lt(time, const(Time(500L))),
          c
        )
      ).head
    )

    conditions shouldEqual Seq(
      Replace(
        Set(c),
        in(
          tuple(time, dimension(Dimensions.KKM_ID)),
          Set((Time(220L), 123456), (Time(330L), 654321))
        )
      )
    )
  }

}
