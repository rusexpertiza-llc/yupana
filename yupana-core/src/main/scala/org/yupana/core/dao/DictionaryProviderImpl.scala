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

package org.yupana.core.dao

import org.yupana.api.schema.Dimension
import org.yupana.core.Dictionary
import org.yupana.core.cache.BoxingTag

class DictionaryProviderImpl[IdType: BoxingTag](dictionaryDao: DictionaryDao[IdType])
    extends DictionaryProvider[IdType] {

  private var dictionaries = Map.empty[Dimension, Dictionary[IdType]]

  override def dictionary(dim: Dimension): Dictionary[IdType] = {
    dictionaries.get(dim) match {
      case Some(d) => d
      case None =>
        val d = new Dictionary(dim, dictionaryDao)
        dictionaries += dim -> d
        d
    }
  }
}
