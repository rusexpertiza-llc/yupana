package org.yupana.utils

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RussianTokenizerTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {
  "Stemmer" should "split numbers and words" in {
    RussianTokenizer.stemmedTokens("95пульсар") should contain theSameElementsAs List("95пульсар", "95", "пульсар")

    RussianTokenizer.stemmedTokens("аи95пульсар") should contain theSameElementsAs List(
      "аи95пульсар",
      "аи",
      "95",
      "пульсар"
    )

    RussianTokenizer.stemmedTokens("аи95") should contain theSameElementsAs List("аи95", "аи", "95")

    RussianTokenizer.stemmedTokens("трк 1 (atum-92-к5) 1.256281 x 39.80") should contain theSameElementsAs List(
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
    RussianTokenizer.stemmedTokens(" лаки дейз ж/р арбуз подушечки 14г (c") should contain allOf ("дейз", "ж/р")

    RussianTokenizer.stemmedTokens("мясн/пр сос.классич с сливк. и/о вар 0,4кг пл/у(аг") should contain("и/о")
  }

  it should "preserve ai" in {
    RussianTokenizer.stemmedTokens("аи") should contain theSameElementsAs List("аи")

    RussianTokenizer.stemmedTokens("аи-95") should contain theSameElementsAs List("аи", "95")

    RussianTokenizer.stemmedTokens("аи-95аи-95-к5") should contain theSameElementsAs List(
      "аи",
      "95а",
      "95",
      "аи",
      "95",
      "к5",
      "к",
      "5"
    )

    RussianTokenizer.stemmedTokens("бензин аи95 n 3") should contain theSameElementsAs List(
      "бензин",
      "аи",
      "95",
      "аи95",
      "n",
      "3"
    )

    RussianTokenizer.stemmedTokens("95аи") should contain theSameElementsAs List("аи", "95", "95а")
  }

  it should "preserve ai with english 'a' and russian 'и' letters" in {
    RussianTokenizer.stemmedTokens("aи") should contain theSameElementsAs List("aи")

    RussianTokenizer.stemmedTokens("aи-95") should contain theSameElementsAs List("aи", "95")

    RussianTokenizer.stemmedTokens("aи-95aи-95-к5") should contain theSameElementsAs List(
      "aи",
      "95a",
      "95",
      "aи",
      "95",
      "к5",
      "к",
      "5"
    )

    RussianTokenizer.stemmedTokens("бензин aи95 n 3") should contain theSameElementsAs List(
      "бензин",
      "aи",
      "95",
      "aи95",
      "n",
      "3"
    )
  }

  it should "preserve letter slash letter abbreviations" in {
    RussianTokenizer.stemmedTokens(";;&х/к") should contain theSameElementsAs List("х", "к", "х/к")

    RussianTokenizer.stemmedTokens("п/п/к") should contain theSameElementsAs List("п", "п", "п/п", "к", "п/п/к")

    RussianTokenizer.stemmedTokens("х/к") should contain theSameElementsAs List("х", "к", "х/к")

    RussianTokenizer.stemmedTokens("qг/qк") should contain theSameElementsAs List("qг", "qк", "qг/qк")

    RussianTokenizer.stemmedTokens("пы/п/к") should contain theSameElementsAs List("пы", "п", "к", "пы/п", "пы/п/к")

    RussianTokenizer.stemmedTokens("п/пы/к") should contain theSameElementsAs List("пы", "п", "к", "п/п", "п/пы/к")

    RussianTokenizer.stemmedTokens("п/п/кы") should contain theSameElementsAs List("кы", "п", "п", "п/п", "п/п/к")

    RussianTokenizer.stemmedTokens("Д/Т") should contain theSameElementsAs List("д", "т", "д/т")

    RussianTokenizer.stemmedTokens("д/т n 1") should contain theSameElementsAs List("д/т", "т", "д", "n", "1")

    RussianTokenizer.stemmedTokens("\"дт фора/-32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "n",
      "1",
      "00000"
    )

    RussianTokenizer.stemmedTokens("\"дт фора/32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "фора/32",
      "n",
      "1",
      "00000"
    )

    RussianTokenizer.stemmedTokens("трk 1:аи-95пульсар-k5 трз0") should contain theSameElementsAs List(
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

    RussianTokenizer.stemmedTokens("\"дт* фора/-32, n 1:00000\"") should contain theSameElementsAs List(
      "дт",
      "фор",
      "32",
      "n",
      "1",
      "00000"
    )

    RussianTokenizer.stemmedTokens("д*т") should contain theSameElementsAs List("д", "т")

    RussianTokenizer.stemmedTokens("д*/т") should contain theSameElementsAs List("д", "т", "/т")
  }

  it should "handle real data correctly" in {
    RussianTokenizer.stemmedTokens("*стельки(шт)") should contain theSameElementsAs List("стельк", "шт")

    RussianTokenizer.stemmedTokens("\"Ж/р\"\"Ригли\"\"джусифрут минис  15,9г\"") should contain theSameElementsAs List(
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

    RussianTokenizer.stemmedTokens(
      "\"АКЦИЯ! Сухарики \"\"ХруcТим багет\"\" 60г\""
    ) should contain theSameElementsAs List(
      "акц",
      "сухарик",
      "хруcт",
      "багет",
      "60",
      "г",
      "60г"
    )

    RussianTokenizer.stemmedTokens("\"Набор: подсвечник \"\"Звезда\"\" + с\"") should contain theSameElementsAs List(
      "набор",
      "подсвечник",
      "звезд",
      "с"
    )
  }

  it should "not split numbers" in {
    RussianTokenizer.stemmedTokens("44.80") should contain theSameElementsAs List("44.80")
    RussianTokenizer.stemmedTokens("76,51") should contain theSameElementsAs List("76,51")
  }

  it should "remove empty tokens after transliteration" in {
    RussianTokenizer.transliteratedTokens("а и ь сидели на ъ-трубе") should contain theSameElementsAs List(
      "a",
      "i",
      "sidel",
      "na",
      "trub"
    )
  }

  it should "provide transliterated tokens" in {
    val data = Table(
      ("Item", "Transliterated"),
      ("00", "00"),
      ("00λλ", "00"),
      ("Ψe0ξ00αβγ", "e 0 e0 00"),
      ("хот-дог датский чикен", "hot dog datsk chiken"),
      ("зёрна кофейные marengo", "zern kofejn marengo"),
      ("мор-ое щербет смор. 80", "mor oe shcherbet smor 80"),
      ("аи-95-к5 евро-6 евро-6", "ai 95 k 5 k5 evr 6 evr 6"),
      ("сигареты пётр i эталон", "sigaret petr i etalon"),
      ("котелок солдатский алю", "kotelok soldatsk alyu"),
      ("Ёлка Зелёная", "elk zelen"),
      ("ЁЁ0Ё", "ee 0 e ee0e"),
      ("ѐe0ѐ", "e 0 e0")
    )

    forAll(data) { (item, expected) =>
      RussianTokenizer.transliteratedTokens(item) mkString " " shouldEqual expected
    }
  }

  it should "provide raw tokens" in {
    val data = Table(
      ("Item", "Transliterated"),
      ("хот-дог датский чикен", "хот дог датский чикен"),
      ("зёрна кофейные marengo", "зёрна кофейные marengo"),
      ("мор-ое щербет смор. 80", "мор ое щербет смор 80"),
      ("аи-95-к5 евро-6 евро-6", "аи 95 к 5 к5 евро 6 евро 6"),
      ("сигареты пётр i эталон", "сигареты пётр i эталон"),
      ("котелок солдатский алю", "котелок солдатский алю"),
      ("Ёлка Зелёная", "ёлка зелёная"),
      ("ЁЁ0Ё", "ёё 0 ё ёё0ё"),
      ("ѐe0ѐ", "e 0 e0")
    )

    forAll(data) { (item, expected) =>
      RussianTokenizer.rawTokens(item) mkString " " shouldEqual expected
    }
  }
}
