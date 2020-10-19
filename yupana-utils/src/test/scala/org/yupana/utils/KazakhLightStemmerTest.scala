package org.yupana.utils

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ FlatSpec, Matchers }

class KazakhLightStemmerTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "KazakhLightStemmer" should "stem strings" in {
    val table = Table(
      ("Original", "Stemmed"),
      ("адамға", "адам"),
      ("мұғалімімізге", "мұғалім"),
      ("пайдаланушыларға", "пайда")
    )

    forAll(table) { (word, stemmed) =>
      KazakhLightStemmer.stem(word) shouldEqual stemmed
    }
  }
}
