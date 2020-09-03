package org.yupana.utils

import org.scalatest.{ FlatSpec, Matchers }
import org.scalatest.prop.TableDrivenPropertyChecks

class TransliteratorTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  "Transliterator" should "stem strings correctly" in {
    val data = Table(
      ("String", "Transliterated"),
      ("черный плащ", "chernyj plashch"),
      ("щупальца южных ёжиков", "shchupalca yuzhnyh ezhikov"),
      ("Ядерный Грибок", "YAdernyj Gribok"),
      ("ОБЪЕДЕННЫЙ КРЕНДЕЛЬ", "OBEDENNYJ KRENDEL")
    )

    forAll(data) { (string, expected) =>
      Transliterator.transliterate(string) shouldEqual expected
    }
  }

  it should "transliterate stemmed items correctly" in {
    val data = Table(
      ("Item", "Transliterated"),
      ("хот-дог датский чикен", "hot dog datsk chiken"),
      ("зёрна кофейные marengo", "zern kofejn marengo"),
      ("мор-ое щербет смор. 80", "mor oe shcherbet smor 80"),
      ("аи-95-к5 евро-6 евро-6", "ai 95 k 5 k5 evr 6 evr 6"),
      ("сигареты пётр i эталон", "sigaret petr i etalon"),
      ("котелок солдатский алю", "kotelok soldatsk alyu"),
      ("Ёлка Зелёная", "elk zelen"),
      ("ЁЁ0Ё", "ee 0 e ee0e")
    )

    forAll(data) { (item, expected) =>
      val tokenized = Tokenizer.stemmedTokens(item) mkString " "
      Transliterator.transliterate(tokenized) shouldEqual expected
    }

  }
}
