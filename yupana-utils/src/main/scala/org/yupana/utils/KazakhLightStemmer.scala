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

import java.util.regex.Pattern

import scala.annotation.tailrec

object KazakhLightStemmer {

  private val PROCESSING_MINIMAL_WORD_LENGTH = 2
  private val _vowelChars = Pattern.compile("[аәоөұүыіеиуёэюя]")

  def stem(s: Array[Char], len: Int): Int = {
    stem(new String(s, 0, len)).length
  }

  @tailrec
  def stem(word: String): String = {
    // don't change short words
    if (word.length <= PROCESSING_MINIMAL_WORD_LENGTH || !continueStemming(word)) {
      word
    } else {
      suffixes.find(s => word endsWith s) match {
        case Some(s) => stem(word.substring(0, word.length - s.length))
        case None    => word
      }
    }
  }

  private def continueStemming(word: String): Boolean = {
    val matcher = _vowelChars.matcher(word)
    // check if there are at least two vowel characters in the word
    matcher.find && matcher.find
  }

  // NOTE: Suffixes order is important. Long suffixes going first, to be cut before short ones.
  private val suffixes = Array( //"сыңдар", "сіңдер","ңыздар", "ңіздер","сыздар", "сіздер",
    "шалық",
    "шелік",
    "даған",
    "деген",
    "таған",
    "теген",
    "лаған",
    "леген",
    "дайын",
    "дейін",
    "тайын",
    "тейін",
    "ңдар",
    "ңдер",
    "дікі",
    "тікі",
    "нікі",
    "атын",
    "етін",
    "йтын",
    "йтін",
    "гелі",
    "қалы",
    "келі",
    "ғалы",
    "шама",
    "шеме",
    "мын",
    "мін",
    "бын",
    "бін",
    "пын",
    "пін",
    "мыз",
    "міз",
    "быз",
    "біз",
    "пыз",
    "піз",
    "сың",
    "сің",
    "сыз",
    "сіз",
    "ңыз",
    "ңіз",
    "дан",
    "ден",
    "тан",
    "тен",
    "нан",
    "нен",
    "нда",
    "нде",
    "дың",
    "дің",
    "тың",
    "тің",
    "ның",
    "нің",
    "дар",
    "дер",
    "тар",
    "тер",
    "лар",
    "лер",
    "бен",
    "пен",
    "мен",
    "дай",
    "дей",
    "тай",
    "тей",
    "дық",
    "дік",
    "тық",
    "тік",
    "лық",
    "лік",
    "паз",
    "ғыш",
    "гіш",
    "қыш",
    "кіш",
    "шек",
    "шақ",
    "шыл",
    "шіл",
    "нші",
    "ншы",
    "дап",
    "деп",
    "тап",
    "теп",
    "лап",
    "леп",
    "даc",
    "деc",
    "таc",
    "теc",
    "лаc",
    "леc",
    "ғар",
    "гер",
    "қар",
    "кер",
    "дыр",
    "дір",
    "тыр",
    "тір",
    "ғыз",
    "гіз",
    "қыз",
    "кіз",
    "ған",
    "ген",
    "қан",
    "кен",
    "ушы",
    "уші",
    "лай",
    "лей",
    "сын",
    "сін",
    "бақ",
    "бек",
    "пақ",
    "пек",
    "мақ",
    "мек",
    "йын",
    "йін",
    "йық",
    "йік",
    "сы",
    "сі",
    "да",
    "де",
    "та",
    "те",
    "ға",
    "ге",
    "қа",
    "ке",
    "на",
    "не",
    "ді",
    "ты",
    "ті",
    "ны",
    "ні",
    "ды",
    "ба",
    "бе",
    "па",
    "пе",
    "ма",
    "ме",
    "лы",
    "лі",
    "ғы",
    "гі",
    "қы",
    "кі",
    "ау",
    "еу",
    "ла",
    "ле",
    "ар",
    "ер",
    "ып",
    "іп",
    "ша",
    "ше",
    "са",
    "се",
    "н",
    "р",
    "п",
    "й",
    "ы",
    "і"
  )
}
