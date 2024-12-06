package org.yupana.externallinks.items

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ AddCondition, Query, RemoveCondition }
import org.yupana.api.utils.CloseableIterator
import org.yupana.cache.CacheFactory
import org.yupana.core._
import org.yupana.core.jit.JIT
import org.yupana.core.model.BatchDataset
import org.yupana.core.auth.YupanaUser
import org.yupana.core.utils.FlatAndCondition
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.externallinks.TestSchema
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }
import org.yupana.settings.Settings
import org.yupana.utils.RussianTokenizer

import java.util.Properties

class RelatedItemsCatalogImplTest extends AnyFlatSpec with Matchers with MockFactory {
  import org.yupana.api.query.syntax.All._

  class MockedTsdb
      extends TSDB(
        TestSchema.schema,
        null,
        null,
        identity,
        SimpleTsdbConfig(),
        { (_: Query, _: String) => NoMetricCollector }
      )

  private val calculator = new ConstantCalculator(RussianTokenizer)

  "RelatedItemsCatalogImpl" should "handle phrase field in conditions" in {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))

    val tsdb = mock[MockedTsdb]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)
    val startTime = Time(System.currentTimeMillis())

    val expQuery1 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("хлеб ржаной"))
    )

    val qc1 = new QueryContext(expQuery1, None, TestSchema.schema.tokenizer, JIT, NoMetricCollector)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery1, startTime, IndexedSeq.empty, YupanaUser.ANONYMOUS)
      .returning({
        val batch = BatchDataset(qc1)

        batch.set(0, dimension(Dimensions.KKM_ID), 123456)
        batch.set(0, time, Time(120))

        batch.set(1, dimension(Dimensions.KKM_ID), 123456)
        batch.set(1, time, Time(150))

        batch.set(2, dimension(Dimensions.KKM_ID), 345112)
        batch.set(2, time, Time(120))

        new TsdbServerResult(qc1, CloseableIterator.pure(Iterator(batch)))
      })

    val expQuery2 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("бородинский"))
    )

    val qc2 = new QueryContext(expQuery2, None, TestSchema.schema.tokenizer, JIT, NoMetricCollector)

    (tsdb.query _)
      .expects(expQuery2, startTime, IndexedSeq.empty, YupanaUser.ANONYMOUS)
      .returning({
        val batch = BatchDataset(qc2)
        batch.set(0, dimension(Dimensions.KKM_ID), 123456)
        batch.set(0, time, Time(125))

        batch.set(1, dimension(Dimensions.KKM_ID), 123456)
        batch.set(1, time, Time(120))

        new TsdbServerResult(qc2, CloseableIterator.pure(Iterator(batch)))
      })

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
      ).head,
      startTime,
      YupanaUser.ANONYMOUS
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
    val startTime = Time(System.currentTimeMillis())

    val expQuery = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(dimension(Dimensions.ITEM)), Set("яйцо молодильное 1к"))
    )

    val qc = new QueryContext(expQuery, None, TestSchema.schema.tokenizer, JIT, NoMetricCollector)

    (tsdb.mapReduceEngine _).expects(*).returning(IteratorMapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery, startTime, IndexedSeq.empty, YupanaUser.ANONYMOUS)
      .returning({
        val batch = BatchDataset(qc)
        batch.set(0, dimension(Dimensions.KKM_ID), 123456)
        batch.set(0, time, Time(220))

        batch.set(1, dimension(Dimensions.KKM_ID), 654321)
        batch.set(1, time, Time(330))

        new TsdbServerResult(qc, CloseableIterator.pure(Iterator(batch)))
      })

    val c = in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.ITEM_FIELD)), Set("яйцо молодильное 1к"))
    val conditions = catalog.transformCondition(
      FlatAndCondition(
        calculator,
        and(
          ge(time, const(Time(100L))),
          lt(time, const(Time(500L))),
          c
        )
      ).head,
      startTime,
      YupanaUser.ANONYMOUS
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
