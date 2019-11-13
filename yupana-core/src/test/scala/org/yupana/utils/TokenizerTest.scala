package org.yupana.utils

import org.scalatest.{ FlatSpec, Matchers }

class TokenizerTest extends FlatSpec with Matchers {
  "Stemmer" should "split numbers and words" in {
    Tokenizer.tokens("95пульсар") should contain theSameElementsAs List("95пульсар", "95", "пульсар")

    Tokenizer.tokens("аи95пульсар") should contain theSameElementsAs List("аи95пульсар", "аи", "95", "пульсар")

    Tokenizer.tokens("аи95") should contain theSameElementsAs List("аи95", "аи", "95")

    Tokenizer.tokens("трк 1 (atum-92-к5) 1.256281 x 39.80") should contain theSameElementsAs List(
      "трк",
      "1",
      "atum",
      "92",
      "к",
      "5",
      "к5",
      "1.256281",
      "x",
      "39.80"
    )
  }

  it should "keep the space" in {
    Tokenizer.tokens(" лаки дейз ж/р арбуз подушечки 14г (c") should contain allOf ("дейз", "ж/р")

    Tokenizer.tokens("мясн/пр сос.классич с сливк. и/о вар 0,4кг пл/у(аг") should contain("и/о")
  }

  it should "preserve ai" in {
    Tokenizer.tokens("аи") should contain theSameElementsAs List("аи")

    Tokenizer.tokens("аи-95") should contain theSameElementsAs List("аи", "95")

    Tokenizer.tokens("аи-95аи-95-к5") should contain theSameElementsAs List(
      "аи",
      "95а",
      "95",
      "аи",
      "95",
      "к5",
      "к",
      "5"
    )

    Tokenizer.tokens("бензин аи95 n 3") should contain theSameElementsAs List("бензин", "аи", "95", "аи95", "n", "3")

    Tokenizer.tokens("95аи") should contain theSameElementsAs List("аи", "95", "95а")
  }

  it should "preserve ai with english 'a' and russian 'и' letters" in {
    Tokenizer.tokens("aи") should contain theSameElementsAs List("aи")

    Tokenizer.tokens("aи-95") should contain theSameElementsAs List("aи", "95")

    Tokenizer.tokens("aи-95aи-95-к5") should contain theSameElementsAs List(
      "aи",
      "95a",
      "95",
      "aи",
      "95",
      "к5",
      "к",
      "5"
    )

    Tokenizer.tokens("бензин aи95 n 3") should contain theSameElementsAs List("бензин", "aи", "95", "aи95", "n", "3")
  }

  it should "preserve letter slash letter abbreviations" in {
    Tokenizer.tokens(";;&х/к") should contain theSameElementsAs List("х", "к", "х/к")

    Tokenizer.tokens("п/п/к") should contain theSameElementsAs List("п", "п", "п/п", "к", "п/п/к")

    Tokenizer.tokens("х/к") should contain theSameElementsAs List("х", "к", "х/к")

    Tokenizer.tokens("qг/qк") should contain theSameElementsAs List("qг", "qк", "qг/qк")

    Tokenizer.tokens("пы/п/к") should contain theSameElementsAs List("пы", "п", "к", "пы/п", "пы/п/к")

    Tokenizer.tokens("п/пы/к") should contain theSameElementsAs List("пы", "п", "к", "п/п", "п/пы/к")

    Tokenizer.tokens("п/п/кы") should contain theSameElementsAs List("кы", "п", "п", "п/п", "п/п/к")

    Tokenizer.tokens("Д/Т") should contain theSameElementsAs List("д", "т", "д/т")

    Tokenizer.tokens("д/т n 1") should contain theSameElementsAs List("д/т", "т", "д", "n", "1")

    Tokenizer.tokens("\"дт фора/-32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "n",
      "1",
      "00000"
    )

    Tokenizer.tokens("\"дт фора/32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "фора/32",
      "n",
      "1",
      "00000"
    )

    Tokenizer.tokens("трk 1:аи-95пульсар-k5 трз0") should contain theSameElementsAs List(
      "трk",
      "1",
      "аи",
      "95пульсар",
      "95",
      "пульсар",
      "k",
      "5",
      "k5",
      "трз0",
      "трз",
      "0"
    )

    Tokenizer.tokens("\"дт* фора/-32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "n",
      "1",
      "00000"
    )

    Tokenizer.tokens("д*т") should contain theSameElementsAs List("д", "т")

    Tokenizer.tokens("д*/т") should contain theSameElementsAs List("д", "т", "/т")
  }

  it should "handle real data correctly" in {
    Tokenizer.tokens("*стельки(шт)") should contain theSameElementsAs List("стельк", "шт")

    Tokenizer.tokens("\"Ж/р\"\"Ригли\"\"джусифрут минис  15,9г\"") should contain theSameElementsAs List(
      "ж",
      "р",
      "ж/р",
      "ригл",
      "джусифрут",
      "минис",
      "г",
      "15,9",
      "15,9г"
    )

    Tokenizer.tokens("\"АКЦИЯ! Сухарики \"\"ХруcТим багет\"\" 60г\"") should contain theSameElementsAs List(
      "акц",
      "сухарик",
      "хруcт",
      "багет",
      "60",
      "г",
      "60г"
    )

    Tokenizer.tokens("\"Набор: подсвечник \"\"Звезда\"\" + с\"") should contain theSameElementsAs List(
      "набор",
      "подсвечник",
      "звезд",
      "с"
    )
  }

  it should "not split numbers" in {
    Tokenizer.tokens("44.80") should contain theSameElementsAs List("44.80")
    Tokenizer.tokens("76,51") should contain theSameElementsAs List("76,51")
  }

  it should "remove empty tokens after transliteration" in {
    Tokenizer.transliteratedTokens("а и ь сидели на ъ-трубе") should contain theSameElementsAs List(
      "a",
      "i",
      "sidel",
      "na",
      "trub"
    )
  }
}
