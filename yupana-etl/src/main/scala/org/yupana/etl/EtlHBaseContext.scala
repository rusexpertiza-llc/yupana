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

package org.yupana.etl

import org.yupana.api.schema.Schema
import org.yupana.core.TSDB
import org.yupana.externallinks.items.ItemsInvertedIndexImpl
import org.yupana.hbase.{ ExternalLinkHBaseConnection, InvertedIndexDaoHBase, Serializers }
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }

trait EtlHBaseContext extends Serializable {
  def tsdb: TSDB
}

object EtlHBaseContext {

  def createAndRegisterInvertedIndex(tsdb: TSDB, cfg: EtlHbaseConfig, schema: Schema): Unit = {
    val hBaseConnection =
      new ExternalLinkHBaseConnection(cfg.tsdbConfig.hBaseConfiguration, cfg.tsdbConfig.hbaseNamespace)
    val invertedIndexDao = new InvertedIndexDaoHBase[String, ItemDimension.KeyType](
      hBaseConnection,
      ItemsInvertedIndexImpl.TABLE_NAME,
      Serializers.stringSerializer,
      Serializers.stringDeserializer,
      Dimensions.ITEM.rStorable.write,
      Dimensions.ITEM.rStorable.read
    )
    val itemsInvertedIndex = new ItemsInvertedIndexImpl(
      schema,
      invertedIndexDao,
      cfg.putIntoInvertedIndex,
      ItemsInvertedIndex
    )
    tsdb.registerExternalLink(ItemsInvertedIndex, itemsInvertedIndex)
  }
}
