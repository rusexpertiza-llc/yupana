package org.yupana.utils

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ FlatSpec, Matchers }

class KazakhTransliteratorTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "KazakhTransliterator" should "transliterate" in {

    val data = Table(
      ("String", "Transliterated"),
      ("черный плащ", "chernyj plashch"),
      ("щупальца южных ёжиков", "shchupalca yuzhnyh ezhikov"),
      ("Ұшырылым «Байқоңыр» ғарыш", "Oshyrylym «Bajkonyr» rarysh"),
      ("басқарылатын жүк кемесін ұшыру Халықаралық", "baskarylatyn zhuk kemesin oshyru Halykaralyk"),
      ("зымыран-ғарыш корпорациясы", "zymyran-rarysh korporaciyasy"),
      ("БАРЛЫҚ ҚАЗАҚСТАНДЫҚТАРДЫ ОСЫ ҚОСЫМШАНЫ", "BARLYK KAZAKSTANDYKTARDY OSY KOSYMSHANY")
    )

    forAll(data) { (string, expected) =>
      KazakhTransliterator.transliterate(string) shouldEqual expected
    }
  }
}
