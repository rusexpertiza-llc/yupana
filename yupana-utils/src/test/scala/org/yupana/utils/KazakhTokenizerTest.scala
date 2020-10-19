package org.yupana.utils

import org.scalatest.{ FlatSpec, Matchers }

class KazakhTokenizerTest extends FlatSpec with Matchers {

  "KazakhTokenizer" should "split words and numbers" in {
    KazakhTokenizer.stemmedTokens("15минуттан") should contain theSameElementsAs List("15", "15минут", "минут")
  }

  it should "transliterate" in {
    KazakhTokenizer.transliteratedTokens("пайдаланушыларға ыңғайлы болу мақсатында") should contain theSameElementsAs List(
      "paj",
      "yn",
      "bolu",
      "mak"
    )
  }

  it should "support both Russian and Kazakh languages" in {
    KazakhTokenizer.transliteratedTokens("летим на черной пчеле - қара арамен ұшу") should contain theSameElementsAs List(
      "let",
      "na",
      "chern",
      "pchel",
      "kara",
      "ara",
      "oshu"
    )
  }
}
