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

package org.yupana.spark

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.yupana.api.schema.DictionaryDimension
import org.yupana.core.Dictionary
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProvider }
import org.yupana.hbase.DictionaryDaoHBase

class SparkDictionaryProvider(config: Config) extends DictionaryProvider with Serializable {
  override def dictionary(dimension: DictionaryDimension): Dictionary = {
    SparkDictionaryProvider.dictionaries.get(dimension) match {
      case Some(d) => d
      case None =>
        CacheFactory.init(config.settings)
        val d = new Dictionary(dimension, localDictionaryDao)
        SparkDictionaryProvider.dictionaries += dimension -> d
        d
    }
  }

  @transient private lazy val localDictionaryDao: DictionaryDao = {
    SparkDictionaryProvider.dictionaryDao match {
      case Some(dao) => dao
      case None =>
        val dao = new DictionaryDaoHBase(
          ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(config)),
          config.hbaseNamespace
        )
        SparkDictionaryProvider.dictionaryDao = Some(dao)
        dao
    }
  }
}

object SparkDictionaryProvider {
  var dictionaryDao = Option.empty[DictionaryDao]
  var dictionaries = Map.empty[DictionaryDimension, Dictionary]
}
