package org.yupana.utils

import org.apache.lucene.analysis.ru.RussianLightStemmer

import scala.collection.mutable

object KazakhTokenizer extends TokenizerBase {
  @transient private lazy val stemmer: KazakhStemmerTokenFilter = new RussianLightStemmer()

  private val includedChars = {
    val charSet = mutable.Set(
      '/', '.', ',', '\\', '%', '*'
    ) ++
      ('0' to '9') ++
      ('a' to 'z') ++
      ('A' to 'Z') ++
      ('а' to 'я') ++
      ('А' to 'Я') +
      'ё' + 'Ё'

    val chars = Array.fill[Boolean](charSet.max + 1)(false)

    charSet.foreach { s =>
      chars(s) = true
    }
    chars
  }

  override def isCharIncluded(ch: Char): Boolean = {
    ch >= includedChars.length || includedChars(ch)
  }

  override def stemArray(array: Array[Char], length: Int): Int = stemmer.stem(array, length)

  override protected def transliterate(s: String): String = KazakhTransliterator.transliterate(s)
}
