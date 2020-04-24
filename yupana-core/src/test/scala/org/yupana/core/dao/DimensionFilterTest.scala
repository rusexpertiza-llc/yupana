//package org.yupana.core.dao
//
//import org.scalatest.{ FlatSpec, Matchers }
//import org.yupana.api.schema.Dimension
//
//class DimensionFilterTest extends FlatSpec with Matchers {
//
//  implicit class RichFilters[A](f: DimensionFilter[A]) {
//    def mat: Map[String, Set[A]] = f.toMap.map { case (k, v) => k.name -> v.toSet }
//  }
//
//  "DimensionFilter" should "combine NoResult with other filters" in {
//    NoResult[Int]() and NoResult() shouldEqual NoResult()
//    NoResult[String]() and EmptyFilter() shouldEqual NoResult()
//    EmptyFilter[Double]() and NoResult() shouldEqual NoResult()
//    NoResult() and DimensionFilter(Dimension("tag"), 1) shouldEqual NoResult()
//    DimensionFilter(Dimension("tag"), Set(3)) and DimensionFilter(Dimension("tag2"), Set(1, 2)) and NoResult() shouldEqual NoResult()
//    NoResult[Int]() or NoResult() shouldEqual NoResult()
//    NoResult[String]() or EmptyFilter() shouldEqual EmptyFilter()
//    EmptyFilter[Long]() or NoResult() shouldEqual EmptyFilter()
//    (NoResult() or DimensionFilter(Dimension("tag"), 1)).mat shouldEqual Map("tag" -> Set(1))
//    ((DimensionFilter(Dimension("tag"), Set(3)) and DimensionFilter(Dimension("tag2"), Set(1, 2))) or NoResult()).mat shouldEqual Map(
//      "tag" -> Set(3),
//      "tag2" -> Set(1, 2)
//    )
//  }
//
//  it should "exclude other filters from NoResult" in {
//    NoResult[Int]() exclude NoResult() shouldEqual NoResult()
//    NoResult[Int]() exclude EmptyFilter() shouldEqual NoResult()
//    NoResult() exclude DimensionFilter(Dimension("tag"), Set(1)) shouldEqual NoResult()
//  }
//
//  it should "combine EmptyFilter with other filters" in {
//    EmptyFilter[Long]() and EmptyFilter() shouldEqual EmptyFilter()
//    val filters = DimensionFilter(Dimension("tag"), Set(4))
//    EmptyFilter() and filters shouldEqual filters
//    filters and EmptyFilter() shouldEqual filters
//
//    EmptyFilter[String]() or EmptyFilter() shouldEqual EmptyFilter()
//    EmptyFilter() or filters shouldEqual EmptyFilter()
//    filters or EmptyFilter() shouldEqual EmptyFilter()
//  }
//
//  it should "exclude other filters from EmptyFilter" in {
//    EmptyFilter[Double]() exclude NoResult() shouldEqual EmptyFilter()
//    EmptyFilter[Int]() exclude EmptyFilter() shouldEqual EmptyFilter()
//    EmptyFilter() exclude DimensionFilter(Dimension("x"), Set(1, 2)) shouldEqual EmptyFilter()
//  }
//
//  it should "combine two filters collections using and and or merge strategies" in {
//    (DimensionFilter(Dimension("a"), Set(1, 2, 3)) and DimensionFilter(Dimension("a"), Set(2, 3, 4))).mat shouldEqual Map(
//      "a" -> Set(2, 3)
//    )
//    DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("a"), Set(3, 4)) shouldEqual NoResult()
//
//    (DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(3, 4))).mat shouldEqual Map(
//      "a" -> Set(1, 2),
//      "b" -> Set(3, 4)
//    )
//    (DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(2, 3)) and DimensionFilter(
//      Dimension("b"),
//      Set(3, 4)
//    )).mat shouldEqual Map("a" -> Set(1, 2), "b" -> Set(3))
//
//    DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(2)) and DimensionFilter(
//      Dimension("b"),
//      Set(3, 4)
//    ) shouldEqual NoResult()
//
//    (DimensionFilter(Dimension("a"), Set(1, 2, 3)) or DimensionFilter(Dimension("a"), Set(2, 3, 4))).mat shouldEqual Map(
//      "a" -> Set(1, 2, 3, 4)
//    )
//    (DimensionFilter(Dimension("a"), Set(1, 2)) or DimensionFilter(Dimension("a"), Set(3, 4))).mat shouldEqual Map(
//      "a" -> Set(1, 2, 3, 4)
//    )
//
//    (DimensionFilter(Dimension("a"), Set(1, 2)) or DimensionFilter(Dimension("b"), Set(3, 4))).mat shouldEqual Map(
//      "a" -> Set(1, 2),
//      "b" -> Set(3, 4)
//    )
//    (DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(2, 3)) or DimensionFilter(
//      Dimension("b"),
//      Set(3, 4)
//    )).mat shouldEqual Map("a" -> Set(1, 2), "b" -> Set(2, 3, 4))
//
//    (DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(2)) or DimensionFilter(
//      Dimension("b"),
//      Set(3, 4)
//    )).mat shouldEqual Map("a" -> Set(1, 2), "b" -> Set(2, 3, 4))
//  }
//
//  it should "exclude from filters collection" in {
//    val filters = DimensionFilter(Dimension("a"), Set(1, 2)) and DimensionFilter(Dimension("b"), Set(3))
//    filters exclude NoResult() shouldEqual filters
//    filters exclude EmptyFilter() shouldEqual filters
//
//    (filters exclude (DimensionFilter(Dimension("a"), Set(1, 3)) and DimensionFilter(Dimension("b"), Set(4)))).mat shouldEqual Map(
//      "a" -> Set(2),
//      "b" -> Set(3)
//    )
//    filters exclude (DimensionFilter(Dimension("a"), Set(1)) and DimensionFilter(Dimension("b"), Set(3, 4))) shouldEqual NoResult()
//    filters exclude (DimensionFilter(Dimension("a"), Set(1, 2, 3)) and DimensionFilter(Dimension("b"), Set(3))) shouldEqual NoResult()
//  }
//}
