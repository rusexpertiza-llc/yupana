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

import org.apache.lucene.analysis.ru.RussianLightStemmer

import scala.collection.mutable

object Tokenizer extends Serializable {
  private val stemmer = new RussianLightStemmer()
  private val charSet = mutable.Set(
    '/', '.', ',', '\\', '%', '*'
  ) ++
    ('0' to '9') ++
    ('a' to 'z') ++
    ('A' to 'Z') ++
    ('а' to 'я') ++
    ('А' to 'Я')

  private val includedChars = Array.fill[Boolean](charSet.max + 1)(false)
  charSet.foreach { s =>
    includedChars(s) = true
  }

  private def isCharIncluded(ch: Char): Boolean = {
    ch >= includedChars.length || includedChars(ch)
  }

  def stem(word: String): String = {
    val w = word.toLowerCase.toCharArray
    val len = stemmer.stem(w, w.length)
    new String(w, 0, len)
  }

  def transliteratedTokens(item: String): Seq[String] = {
    stemmedTokens(item).map(Transliterator.transliterate).filterNot(_.isEmpty)
  }

  def stemmedTokens(item: String): Seq[String] = {
    tokenize(item, stemmer.stem)
  }

  def tokens(item: String): Seq[String] = {
    tokenize(item, (_, x) => x)
  }

  private def tokenize(item: String, tokenLength: (Array[Char], Int) => Int): Seq[String] = {
    val wordsList = new mutable.ListBuffer[String]()

    def sliceStemAppend(from: Int, to: Int, offset: Int, updated: Array[Char]): Unit = {
      if (from < to) {
        val off = math.max(offset, 0)
        val updatedFrom = if (from != 0) from - off else from
        val length = to - (if (from != 0) from else off)
        val word = new Array[Char](length)
        Array.copy(updated, updatedFrom, word, 0, length)
        val newLength = tokenLength(word, word.length)
        val newWordArr = new Array[Char](newLength)
        Array.copy(word, 0, newWordArr, 0, newLength)
        val newWord = new String(newWordArr)
        if (newWord != " " && newWord.nonEmpty) {
          wordsList += newWord
        }
      }
    }

    def isWhiteSpace(ch: Char) = ch == ' ' || ch == '\t'

    var from = 0
    var originFrom = 0
    var offset = 0

    val updated = Array.fill[Char](item.length)(' ')
    (0 until item.length).foreach { i =>
      val ch = item(i)
      val charIncluded = isCharIncluded(ch)
      if (charIncluded) {
        updated(i - offset) = ch.toLower
      }

      if (isWhiteSpace(ch)) {
        // Основной разделитель
        sliceStemAppend(from, i, offset, updated)
        if (from != originFrom) {
          sliceStemAppend(originFrom, i, offset, updated)
        }
        from = i + 1
        originFrom = i + 1
      } else {
        val isLetter = ch.isLetter
        val isDigit = ch.isDigit
        val prev: Char = if (i > 0) item(i - 1) else ' '
        val next: Char = if (i < item.length - 1) item(i + 1) else ' '
        val prevIsDigit = prev.isDigit
        val prevIsLetter = prev.isLetter
        val nextIsDigit = next.isDigit

        if ((prevIsDigit && isLetter) || (prevIsLetter && isDigit)) {
          // Разделители с сохранением оригинала и разделителя
          sliceStemAppend(from, i, offset, updated)
          from = i
        } else if (ch == '/' || ch == '%' || ch == '\\' || (ch == ',' && !(prevIsDigit && nextIsDigit)) || (ch == '*' && prevIsDigit && nextIsDigit)) {
          // Разделители с сохранением оригинала и исключением самого разделителя
          sliceStemAppend(from, i, offset, updated)
          if (from != originFrom) {
            sliceStemAppend(originFrom, i, offset, updated)
          }
          from = i + 1
          if ((isWhiteSpace(next) && ch != '%') || !isCharIncluded(next)) {
            originFrom = from
          }
        } else if (ch == ':' && prevIsDigit && !nextIsDigit) {
          // Удаление групп
          sliceStemAppend(from, i, offset, updated)
          if (from != originFrom) {
            sliceStemAppend(originFrom, i, offset, updated)
          }
          from = i + 1
          originFrom = i + 1
        } else if (!charIncluded || ((ch == '*' || ch == '.') && (!prevIsDigit || !nextIsDigit))) {
          // Разделители с исключением оригинала и разделителя
          sliceStemAppend(from, i, offset, updated)
          if (from != originFrom) {
            sliceStemAppend(originFrom, i, offset, updated)
          }
          from = i + 1
          originFrom = i + 1
        }
      }
      if (!charIncluded) {
        offset += 1
      }
    }

    if (from < item.length) {
      sliceStemAppend(from, item.length, offset, updated)
    }
    if (originFrom < item.length && originFrom != from) {
      sliceStemAppend(originFrom, item.length, offset, updated)
    }
    wordsList
  }
}
