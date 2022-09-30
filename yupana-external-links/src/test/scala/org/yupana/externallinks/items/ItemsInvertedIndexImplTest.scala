package org.yupana.externallinks.items

import java.util.Properties
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.yupana.api.query.{ DimIdInExpr, DimIdNotInExpr, Replace }
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.InvertedIndexDao
import org.yupana.core.{ ConstantCalculator, TSDB }
import org.yupana.externallinks.TestSchema
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.utils.TimeBoundedCondition
import org.yupana.utils.RussianTokenizer

class ItemsInvertedIndexImplTest
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Inside {

  private val calculator = new ConstantCalculator(RussianTokenizer)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties)
  }

  override protected def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  "ItemsInvertedIndex" should "substitute in condition" in withMocks { (index, dao, _) =>
    import org.yupana.api.query.syntax.All._

    (dao.values _).expects("kolbas").returning(si("колбаса копчения", "колбаса вареная", "колбаса вареная молочная"))
    (dao.values _).expects("varen").returning(si("колбаса копчения", "колбаса вареная", "колбаса вареная молочная"))
    (dao.values _).expects("shchupalc").returning(si("щупальца краба", "щупальца кальмара"))
    (dao.values _).expects("kalmar").returning(si("щупальца кальмара", "щупальца кальмара", "кальмар красный"))
    (dao.values _).expects("hol").returning(si("мясо хол", "колбаса хол копчения"))
    (dao.values _).expects("kopchen").returning(si("колбаса хол копчения", "рыба копченая"))

    val c1 = in(
      lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)),
      Set("колбаса вареная", "щупальца кальмара")
    )
    val c2 = neq(
      lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)),
      const("хол копчения")
    )
    val actual = index.transformCondition(
      TimeBoundedCondition(
        calculator,
        and(
          c1,
          c2
        )
      ).head
    )

    actual shouldEqual Seq(
      Replace(
        Set(c1),
        DimIdInExpr(Dimensions.ITEM, si("колбаса вареная", "колбаса вареная молочная", "щупальца кальмара"))
      ),
      Replace(
        Set(c2),
        DimIdNotInExpr(Dimensions.ITEM, si("колбаса хол копчения"))
      )
    )
  }

  it should "put values storage" in withMocks { (index, dao, tsdb) =>

    (dao.batchPut _).expects(where { vs: Map[String, Set[ItemDimension.KeyType]] =>
      vs.keySet == Set("sigaret", "legk", "molok", "papiros")
    })

    index.putItemNames(Set("сигареты легкие", "ПаПиросы", "молоко"))
  }

  it should "ignore handle prefixes" in withMocks { (index, dao, _) =>
    import org.yupana.api.query.syntax.All._

    (dao.values _).expects("krasn").returning(si("красный болт", "красное яблоко", "еще красное яблоко"))
    (dao.values _).expects("yablok").returning(si("еще красное яблоко", "красное яблоко", "сок яблоко"))
    (dao.values _).expects("zhelt").returning(si("желтый банан"))
    (dao.valuesByPrefix _).expects("banan").returning(si("желтый банан", "зеленый банан"))
    val res = index.transformCondition(
      TimeBoundedCondition(
        calculator,
        in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("красное яблоко", "банан% желтый"))
      ).head
    )

    inside(res) {
      case Seq(Replace(_, Seq(DimIdInExpr(d, vs)))) =>
        d shouldEqual Dimensions.ITEM
        vs.toList should contain theSameElementsInOrderAs si(
          "красное яблоко",
          "еще красное яблоко",
          "желтый банан"
        ).toList
    }
  }

  it should "ignore empty prefixes" in withMocks { (index, dao, _) =>
    import org.yupana.api.query.syntax.All._

    (dao.values _).expects("sigaret").returning(si("сигареты винстон", "сигареты бонд"))

    val res = index.transformCondition(
      TimeBoundedCondition(
        calculator,
        notIn(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("сигареты %"))
      ).head
    )

    inside(res) {
      case Seq(Replace(_, Seq(DimIdNotInExpr(d, vs)))) =>
        d shouldEqual Dimensions.ITEM
        vs.toSeq should contain theSameElementsInOrderAs si("сигареты винстон", "сигареты бонд").toList
    }
  }

  private def si(ls: String*): SortedSetIterator[ItemDimension.KeyType] = {
    val s = ls.map(Dimensions.ITEM.hashFunction).sortWith(Dimensions.ITEM.rOrdering.lt)
    SortedSetIterator(s.iterator)
  }

  def withMocks(body: (ItemsInvertedIndexImpl, InvertedIndexDao[String, ItemDimension.KeyType], TSDB) => Unit): Unit = {

    val dao = mock[InvertedIndexDao[String, ItemDimension.KeyType]]
    val tsdb = mock[TSDB]
    val index = new ItemsInvertedIndexImpl(
      TestSchema.schema,
      dao,
      false,
      ItemsInvertedIndex
    )

    body(index, dao, tsdb)
  }
}
