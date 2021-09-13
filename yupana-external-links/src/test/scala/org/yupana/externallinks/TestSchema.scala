package org.yupana.externallinks

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric, RawDimension, Schema, Table }
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }

object TestSchema {
  val xDim = RawDimension[Int]("X")
  val yDim = RawDimension[Int]("Y")

  val aMetric = Metric[String]("A", 1)

  object TestLink extends ExternalLink {

    val field1 = "field1"
    val field2 = "field2"
    val field3 = "field3"

    override type DimType = Int
    override val linkName: String = "Test"
    override val dimension: Dimension.Aux[Int] = xDim
    override val fields: Set[LinkField] = Set(field1, field2, field3).map(LinkField[String])
  }

  val table: Table = new Table(
    "test",
    1000,
    Seq(xDim, yDim),
    Seq(aMetric),
    Seq(TestLink),
    new LocalDateTime(2016, 1, 1, 0, 0).toDateTime(DateTimeZone.UTC).getMillis
  )

  val schema: Schema =
    Schema(Seq(table), Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
}
