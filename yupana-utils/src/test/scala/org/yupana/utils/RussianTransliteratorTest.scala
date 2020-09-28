package org.yupana.utils

import org.scalatest.{ FlatSpec, Matchers }
import org.scalatest.prop.TableDrivenPropertyChecks

class RussianTransliteratorTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  "Transliterator" should "transliterate strings correctly" in {
    val data = Table(
      ("String", "Transliterated"),
      ("черный плащ", "chernyj plashch"),
      ("щупальца южных ёжиков", "shchupalca yuzhnyh ezhikov"),
      ("Ядерный Грибок", "YAdernyj Gribok"),
      ("ОБЪЕДЕННЫЙ КРЕНДЕЛЬ", "OBEDENNYJ KRENDEL")
    )

    forAll(data) { (string, expected) =>
      RussianTransliterator.transliterate(string) shouldEqual expected
    }
  }
}
