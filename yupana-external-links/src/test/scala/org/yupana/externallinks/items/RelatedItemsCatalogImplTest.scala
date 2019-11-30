package org.yupana.externallinks.items

import org.scalamock.scalatest.MockFactory
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core.{ MapReducible, QueryContext, TSDB, TsdbServerResult }
import org.yupana.schema.{ Dimensions, Tables }
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RelatedItemsCatalogImplTest extends AnyFlatSpec with Matchers with MockFactory {
  import org.yupana.api.query.syntax.All._

  "RelatedItemsCatalogImpl" should "handle phrase field in conditions" in {
    val tsdb = mock[TSDB]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery1 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID_TAG).toField, time.toField),
      in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("хлеб ржаной"))
    )

    val qc1 = QueryContext(expQuery1, const(true))

    (tsdb.mapReduceEngine _).expects(*).returning(MapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery1)
      .returning(
        new TsdbServerResult(
          qc1,
          Seq(
            Array[Option[Any]](Some("123456"), Some(Time(120))),
            Array[Option[Any]](Some("123456"), Some(Time(150))),
            Array[Option[Any]](Some("345112"), Some(Time(120)))
          ).toIterator
        )
      )

    val expQuery2 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID_TAG).toField, time.toField),
      in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("бородинский"))
    )

    val qc2 = QueryContext(expQuery2, const(true))

    (tsdb.query _)
      .expects(expQuery2)
      .returning(
        new TsdbServerResult(
          qc2,
          Seq(
            Array[Option[Any]](Some("123456"), Some(Time(125))),
            Array[Option[Any]](Some("123456"), Some(Time(120)))
          ).toIterator
        )
      )

    val condition = catalog.condition(
      and(
        ge(time, const(Time(100L))),
        lt(time, const(Time(500L))),
        in(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELDS), Set("хлеб ржаной")),
        notIn(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELDS), Set("бородинский"))
      )
    )

    condition shouldEqual and(
      ge(time, const(Time(100L))),
      lt(time, const(Time(500L))),
      in(
        tuple(time, dimension(Dimensions.KKM_ID_TAG)),
        Set((Time(120L), "123456"), (Time(150L), "123456"), (Time(120L), "345112"))
      ),
      notIn(
        tuple(time, dimension(Dimensions.KKM_ID_TAG)),
        Set((Time(125L), "123456"), (Time(120L), "123456"))
      )
    )
  }

  it should "handle item field in conditions" in {
    val tsdb = mock[TSDB]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID_TAG).toField, time.toField),
      in(dimension(Dimensions.ITEM_TAG), Set("яйцо молодильное 1к"))
    )

    val qc = QueryContext(expQuery, const(true))

    (tsdb.mapReduceEngine _).expects(*).returning(MapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery)
      .returning(
        new TsdbServerResult(
          qc,
          Seq(
            Array[Option[Any]](Some("123456"), Some(Time(220))),
            Array[Option[Any]](Some("654321"), Some(Time(330)))
          ).toIterator
        )
      )

    val condition = catalog.condition(
      and(
        ge(time, const(Time(100L))),
        lt(time, const(Time(500L))),
        in(link(RelatedItemsCatalog, RelatedItemsCatalog.ITEM_FIELD), Set("яйцо молодильное 1к"))
      )
    )

    condition shouldEqual and(
      ge(time, const(Time(100L))),
      lt(time, const(Time(500L))),
      in(
        tuple(time, dimension(Dimensions.KKM_ID_TAG)),
        Set((Time(220L), "123456"), (Time(330L), "654321"))
      )
    )
  }

}
