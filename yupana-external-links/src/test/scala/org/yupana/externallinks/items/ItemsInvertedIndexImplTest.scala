package org.yupana.externallinks.items

import java.util.Properties

import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Inside, Matchers }
import org.yupana.api.query.{ DimIdInExpr, DimIdNotInExpr }
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.{ Dictionary, TSDB }
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, InvertedIndexDao }
import org.yupana.schema.Dimensions
import org.yupana.schema.externallinks.ItemsInvertedIndex

class ItemsInvertedIndexImplTest
    extends FlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Inside {

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")
  }

  override protected def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  "ItemsInvertedIndex" should "substitute in condition" in withMocks { (index, dao, _) =>
    import org.yupana.api.query.syntax.All._

    (dao.values _).expects("kolbas").returning(SortedSetIterator(1, 4, 5))
    (dao.values _).expects("varen").returning(SortedSetIterator(2, 4, 5))
    (dao.values _).expects("shchupalc").returning(SortedSetIterator(1, 42))
    (dao.values _).expects("kalmar").returning(SortedSetIterator(42, 45, 48))
    (dao.values _).expects("hol").returning(SortedSetIterator(1, 2))
    (dao.values _).expects("kopchen").returning(SortedSetIterator(2, 3))

    val actual = index.condition(
      and(
        in(
          link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD),
          Set("колбаса вареная", "щупальца кальмара")
        ),
        neq(
          link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD),
          const("хол копчения")
        )
      )
    )

    actual shouldEqual and(
      DimIdInExpr(dimension(Dimensions.ITEM_TAG), SortedSetIterator(4, 5, 42)),
      DimIdNotInExpr(dimension(Dimensions.ITEM_TAG), SortedSetIterator(2))
    )
  }

  it should "put values storage" in withMocks { (index, dao, tsdb) =>
    val dictDao = mock[DictionaryDao]
    val itemDict = new Dictionary(Dimensions.ITEM_TAG, dictDao)
    (tsdb.dictionary _).expects(Dimensions.ITEM_TAG).returning(itemDict)
    (dictDao.getIdsByValues _)
      .expects(Dimensions.ITEM_TAG, Set("сигареты легкие", "папиросы", "молоко"))
      .returning(Map("папиросы" -> 2))
    (dictDao.createSeqId _).expects(Dimensions.ITEM_TAG).returning(3)
    (dictDao.checkAndPut _).expects(Dimensions.ITEM_TAG, *, "сигареты легкие").returning(true)
    (dictDao.createSeqId _).expects(Dimensions.ITEM_TAG).returning(4)
    (dictDao.checkAndPut _).expects(Dimensions.ITEM_TAG, *, "молоко").returning(true)

    (dao.batchPut _).expects(where { vs: Map[String, Set[Long]] =>
      vs.keySet == Set("sigaret", "legk", "molok", "papiros")
    })

    index.putItemNames(Set("сигареты легкие", "папиросы", "молоко"))
  }

  it should "handle queries" in withMocks { (index, dao, _) =>
    import org.yupana.api.query.syntax.All._

    (dao.values _).expects("krasn").returning(SortedSetIterator(1, 2, 3))
    (dao.values _).expects("yablok").returning(SortedSetIterator(2, 3, 5))
    (dao.values _).expects("zhelt").returning(SortedSetIterator(6))
    (dao.valuesByPrefix _).expects("banan").returning(SortedSetIterator(6, 7))
    val res = index.condition(
      in(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), Set("красное яблоко", "банан% желтый"))
    )

    inside(res) {
      case DimIdInExpr(d, vs) =>
        d shouldEqual dimension(Dimensions.ITEM_TAG)
        vs.toSeq should contain theSameElementsInOrderAs Seq(2L, 3L, 6L)
    }
  }

  def withMocks(body: (ItemsInvertedIndexImpl, InvertedIndexDao[String, Long], TSDB) => Unit): Unit = {

    val dao = mock[InvertedIndexDao[String, Long]]
    val tsdb = mock[TSDB]
    val index = new ItemsInvertedIndexImpl(tsdb, dao, ItemsInvertedIndex)

    body(index, dao, tsdb)
  }
}
