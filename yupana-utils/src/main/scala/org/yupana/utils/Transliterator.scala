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

object Transliterator {
  private val chars: Array[String] = (Char.MinValue to Char.MaxValue).map(_.toString).toArray

  private val russianTable = Map(
    'а' -> "a",
    'б' -> "b",
    'в' -> "v",
    'г' -> "g",
    'д' -> "d",
    'е' -> "e",
    'ё' -> "e",
    'ж' -> "zh",
    'з' -> "z",
    'и' -> "i",
    'й' -> "j",
    'к' -> "k",
    'л' -> "l",
    'м' -> "m",
    'н' -> "n",
    'о' -> "o",
    'п' -> "p",
    'р' -> "r",
    'с' -> "s",
    'т' -> "t",
    'у' -> "u",
    'ф' -> "f",
    'х' -> "h",
    'ц' -> "c",
    'ч' -> "ch",
    'ш' -> "sh",
    'щ' -> "shch",
    'ъ' -> "",
    'ы' -> "y",
    'ь' -> "",
    'э' -> "e",
    'ю' -> "yu",
    'я' -> "ya",
    'А' -> "A",
    'Б' -> "B",
    'В' -> "V",
    'Г' -> "G",
    'Д' -> "D",
    'Е' -> "E",
    'Ё' -> "E",
    'Ж' -> "ZH",
    'З' -> "Z",
    'И' -> "I",
    'Й' -> "J",
    'К' -> "K",
    'Л' -> "L",
    'М' -> "M",
    'Н' -> "N",
    'О' -> "O",
    'П' -> "P",
    'Р' -> "R",
    'С' -> "S",
    'Т' -> "T",
    'У' -> "U",
    'Ф' -> "F",
    'Х' -> "H",
    'Ц' -> "C",
    'Ч' -> "CH",
    'Ш' -> "SH",
    'Щ' -> "SHCH",
    'Ъ' -> "",
    'Ы' -> "Y",
    'Ь' -> "",
    'Э' -> "E",
    'Ю' -> "YU",
    'Я' -> "YA"
  )

  russianTable.foreach { case (c, s) => chars(c) = s }

  def transliterate(s: String): String = {
    val builder = new java.lang.StringBuilder(s.length * 2)
    s.foreach { c => builder.append(chars(c)) }
    builder.toString
  }
}
