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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.yupana.api.schema.Schema
import org.yupana.api.utils.Tokenizer
import org.yupana.core.{ ExpressionCalculator, TSDB }
import org.yupana.externallinks.items.ItemsInvertedIndexImpl
import org.yupana.hbase.{ ExternalLinkHBaseConnection, InvertedIndexDaoHBase, Serializers, TSDBHBase }
import org.yupana.schema.{ Dimensions, ItemDimension }
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.utils.{ RussianTokenizer, RussianTransliterator }

class EtlContext(val cfg: EtlConfig, schema: Schema, tokenizer: Tokenizer) extends Serializable {
  def hBaseConfiguration: Configuration = {
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", cfg.hbaseZookeeper)
    hbaseconf.set("zookeeper.session.timeout", 180000.toString)
    cfg.hbaseWriteBufferSize.foreach(x => hbaseconf.set("hbase.client.write.buffer", x.toString))
    hbaseconf
  }

  private def init: TSDB = {
    val calculator = new ExpressionCalculator(tokenizer)
    val tsdb =
      TSDBHBase(hBaseConfiguration, cfg.hbaseNamespace, schema, identity, cfg.properties, tokenizer, calculator, cfg)
    setup(tsdb)
    EtlContext.tsdb = Some(tsdb)
    tsdb
  }

  protected def setup(tsdbInstance: TSDB): Unit = {
    val hBaseConnection = new ExternalLinkHBaseConnection(hBaseConfiguration, cfg.hbaseNamespace)
    val invertedIndexDao = new InvertedIndexDaoHBase[String, ItemDimension.KeyType](
      hBaseConnection,
      ItemsInvertedIndexImpl.TABLE_NAME,
      Serializers.stringSerializer,
      Serializers.stringDeserializer,
      Dimensions.ITEM.rStorable.write,
      Dimensions.ITEM.rStorable.read
    )
    val itemsInvertedIndex = new ItemsInvertedIndexImpl(
      invertedIndexDao,
      cfg.putIntoInvertedIndex,
      ItemsInvertedIndex,
      RussianTokenizer,
      RussianTransliterator
    )
    tsdbInstance.registerExternalLink(ItemsInvertedIndex, itemsInvertedIndex)
  }

  @transient lazy val tsdb: TSDB = EtlContext.tsdb.getOrElse(init)
}

object EtlContext {
  private var tsdb: Option[TSDB] = None
}
