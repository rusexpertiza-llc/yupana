/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.utils

import scala.collection.mutable

object KazakhTokenizer extends TokenizerBase {
  @transient private lazy val stemmer: KazakhLightStemmer = new KazakhLightStemmer()

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
