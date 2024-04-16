package org.yupana.externallinks

import org.yupana.api.schema.{ DictionaryDimension, ExternalLink, LinkField, RawDimension, Schema, Table }
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }

import java.time.{ LocalDateTime, ZoneOffset }

object TestSchema {
  val xDim = DictionaryDimension("X")
  val yDim = RawDimension[Int]("Y")

  object TestLink extends ExternalLink {

    val field1 = "field1"
    val field2 = "field2"
    val field3 = "field3"

    override type DimType = String
    override val linkName: String = "Test"
    override val dimension: DictionaryDimension = xDim
    override val fields: Set[LinkField] = Set(field1, field2, field3).map(LinkField[String])
  }

  val table: Table = new Table(
    "test",
    1000,
    Seq(xDim, yDim),
    Seq.empty,
    Seq(TestLink),
    LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
  )

  val schema: Schema =
    Schema(Seq(table), Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
}
