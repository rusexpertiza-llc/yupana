package org.yupana.utils

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RussianTransliteratorTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

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
