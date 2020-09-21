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

import org.yupana.api.utils.ItemFixer

object OfdItemFixer extends ItemFixer {
  private val replacements: Seq[(String, String)] = Seq(
    "┬л" -> "\"",
    "┬╗" -> "\"",
    "╕" -> "ё",
    "╣" -> "№"
  )

  override def fix(s: String): String = replacements.foldLeft(s)((a, r) => a.replace(r._1, r._2))
}
