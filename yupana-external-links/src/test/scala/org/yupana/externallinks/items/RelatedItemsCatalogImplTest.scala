package org.yupana.externallinks.items

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ AddCondition, Query, RemoveCondition }
import org.yupana.core._
import org.yupana.core.jit.JIT
import org.yupana.core.model.InternalRowBuilder
import org.yupana.core.utils.FlatAndCondition
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.externallinks.TestSchema
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }
import org.yupana.utils.RussianTokenizer

class RelatedItemsCatalogImplTest extends AnyFlatSpec with Matchers with MockFactory {
  import org.yupana.api.query.syntax.All._

  class MockedTsdb
      extends TSDB(TestSchema.schema, null, null, identity, SimpleTsdbConfig(), { _: Query => NoMetricCollector })

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

    val qc1 = new QueryContext(expQuery1, None, JIT, NoMetricCollector)
    val rowBuilder1 = new InternalRowBuilder(qc1)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery1)
      .returning(
        new TsdbServerResult(
          qc1,
          rowBuilder1,
          Seq(
            rowBuilder1
              .set(dimension(Dimensions.KKM_ID), 123456)
              .set(time, Time(120))
              .buildAndReset(),
            rowBuilder1
              .set(dimension(Dimensions.KKM_ID), 123456)
              .set(time, Time(150))
              .buildAndReset(),
            rowBuilder1
              .set(dimension(Dimensions.KKM_ID), 345112)
              .set(time, Time(120))
              .buildAndReset()
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

    val qc2 = new QueryContext(expQuery2, None, JIT, NoMetricCollector)
    val rowBuilder2 = new InternalRowBuilder(qc2)

    (tsdb.query _)
      .expects(expQuery2)
      .returning(
        new TsdbServerResult(
          qc2,
          rowBuilder2,
          Seq(
            rowBuilder2
              .set(dimension(Dimensions.KKM_ID), 123456)
              .set(time, Time(125))
              .buildAndReset(),
            rowBuilder2
              .set(dimension(Dimensions.KKM_ID), 123456)
              .set(time, Time(120))
              .buildAndReset()
          ).iterator
        )
      )

    val c1 = in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("хлеб ржаной"))
    val c2 = notIn(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("бородинский"))

    val conditions = catalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(100L))),
          lt(time, const(Time(500L))),
          c1,
          c2
        )
      ).head
    )

    conditions should contain theSameElementsAs Seq(
      RemoveCondition(c1),
      AddCondition(
        in(
          tuple(time, dimension(Dimensions.KKM_ID)),
          Set((Time(120L), 123456), (Time(150L), 123456), (Time(120L), 345112))
        )
      ),
      RemoveCondition(c2),
      AddCondition(
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

    val qc = new QueryContext(expQuery, None, JIT, NoMetricCollector)
    val rowBuilder = new InternalRowBuilder(qc)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery)
      .returning(
        new TsdbServerResult(
          qc,
          rowBuilder,
          Seq(
            rowBuilder
              .set(dimension(Dimensions.KKM_ID), 123456)
              .set(time, Time(220))
              .buildAndReset(),
            rowBuilder
              .set(dimension(Dimensions.KKM_ID), 654321)
              .set(time, Time(330))
              .buildAndReset()
          ).iterator
        )
      )

    val c = in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.ITEM_FIELD)), Set("яйцо молодильное 1к"))
    val conditions = catalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(100L))),
          lt(time, const(Time(500L))),
          c
        )
      ).head
    )

    conditions shouldEqual Seq(
      RemoveCondition(c),
      AddCondition(
        in(
          tuple(time, dimension(Dimensions.KKM_ID)),
          Set((Time(220L), 123456), (Time(330L), 654321))
        )
      )
    )
  }

}
