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

import org.yupana.api.utils.Transliterator

class TableTransliterator(table: Map[Char, String]) extends Transliterator {
  private val chars: Array[String] = (Char.MinValue to Char.MaxValue).map(_.toString).toArray
  table.foreach { case (c, s) => chars(c) = s }

  def transliterate(s: String): String = {
    val builder = new java.lang.StringBuilder(s.length * 2)
    s.foreach { c =>
      builder.append(chars(c))
    }
    builder.toString
  }
}
