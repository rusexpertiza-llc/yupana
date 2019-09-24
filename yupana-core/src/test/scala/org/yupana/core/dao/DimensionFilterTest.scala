package org.yupana.core.dao

import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.schema.Dimension

class DimensionFilterTest extends FlatSpec with Matchers {

  "DimensionFilter" should "combine NoResult with other filters" in {
    NoResult[Int]() and NoResult() shouldEqual NoResult()
    NoResult[String]() and EmptyFilter() shouldEqual NoResult()
    EmptyFilter[Double]() and NoResult() shouldEqual NoResult()
    NoResult() and FiltersCollection(Map(Dimension("tag") -> Set(1))) shouldEqual NoResult()
    FiltersCollection(Map(Dimension("tag") -> Set(3), Dimension("tag2") -> Set(1, 2))) and NoResult() shouldEqual NoResult()

    NoResult[Int]() or NoResult() shouldEqual NoResult()
    NoResult[String]() or EmptyFilter() shouldEqual EmptyFilter()
    EmptyFilter[Long]() or NoResult() shouldEqual EmptyFilter()
    NoResult() or FiltersCollection(Map(Dimension("tag") -> Set(1))) shouldEqual FiltersCollection(
      Map(Dimension("tag") -> Set(1))
    )
    FiltersCollection(Map(Dimension("tag") -> Set(3), Dimension("tag2") -> Set(1, 2))) or NoResult() shouldEqual FiltersCollection(
      Map(Dimension("tag") -> Set(3), Dimension("tag2") -> Set(1, 2))
    )
  }

  it should "exclude other filters from NoResult" in {
    NoResult[Int]() exclude NoResult() shouldEqual NoResult()
    NoResult[Int]() exclude EmptyFilter() shouldEqual NoResult()
    NoResult() exclude FiltersCollection(Map(Dimension("tag") -> Set(1))) shouldEqual NoResult()
  }

  it should "combine EmptyFilter with other filters" in {
    EmptyFilter[Long]() and EmptyFilter() shouldEqual EmptyFilter()
    val filters = FiltersCollection(Map(Dimension("tag") -> Set(4)))
    EmptyFilter() and filters shouldEqual filters
    filters and EmptyFilter() shouldEqual filters

    EmptyFilter[String]() or EmptyFilter() shouldEqual EmptyFilter()
    EmptyFilter() or filters shouldEqual EmptyFilter()
    filters or EmptyFilter() shouldEqual EmptyFilter()
  }

  it should "exclude other filters from EmptyFilter" in {
    EmptyFilter[Double]() exclude NoResult() shouldEqual EmptyFilter()
    EmptyFilter[Int]() exclude EmptyFilter() shouldEqual EmptyFilter()
    EmptyFilter() exclude FiltersCollection(Map(Dimension("x") -> Set(1, 2))) shouldEqual EmptyFilter()
  }

  it should "combine two filters collections using and and or merge strategies" in {
    FiltersCollection(Map(Dimension("a") -> Set(1, 2, 3))) and FiltersCollection(Map(Dimension("a") -> Set(2, 3, 4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(2, 3))
    )
    FiltersCollection(Map(Dimension("a") -> Set(1, 2))) and FiltersCollection(Map(Dimension("a") -> Set(3, 4))) shouldEqual NoResult()

    FiltersCollection(Map(Dimension("a") -> Set(1, 2))) and FiltersCollection(Map(Dimension("b") -> Set(3, 4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(3, 4))
    )
    FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2, 3))) and FiltersCollection(
      Map(Dimension("b") -> Set(3, 4))
    ) shouldEqual FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(3)))

    FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2))) and FiltersCollection(
      Map(Dimension("b") -> Set(3, 4))
    ) shouldEqual NoResult()

    FiltersCollection(Map(Dimension("a") -> Set(1, 2, 3))) or FiltersCollection(Map(Dimension("a") -> Set(2, 3, 4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(1, 2, 3, 4))
    )
    FiltersCollection(Map(Dimension("a") -> Set(1, 2))) or FiltersCollection(Map(Dimension("a") -> Set(3, 4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(1, 2, 3, 4))
    )

    FiltersCollection(Map(Dimension("a") -> Set(1, 2))) or FiltersCollection(Map(Dimension("b") -> Set(3, 4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(3, 4))
    )
    FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2, 3))) or FiltersCollection(
      Map(Dimension("b") -> Set(3, 4))
    ) shouldEqual FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2, 3, 4)))

    FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2))) or FiltersCollection(
      Map(Dimension("b") -> Set(3, 4))
    ) shouldEqual FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(2, 3, 4)))
  }

  it should "exclude from filters collection" in {
    val filters = FiltersCollection(Map(Dimension("a") -> Set(1, 2), Dimension("b") -> Set(3)))
    filters exclude NoResult() shouldEqual filters
    filters exclude EmptyFilter() shouldEqual filters

    filters exclude FiltersCollection(Map(Dimension("a") -> Set(1, 3), Dimension("b") -> Set(4))) shouldEqual FiltersCollection(
      Map(Dimension("a") -> Set(2), Dimension("b") -> Set(3))
    )
    filters exclude FiltersCollection(Map(Dimension("a") -> Set(1), Dimension("b") -> Set(3, 4))) shouldEqual NoResult()
    filters exclude FiltersCollection(Map(Dimension("a") -> Set(1, 2, 3), Dimension("b") -> Set(3))) shouldEqual NoResult()
  }
}
