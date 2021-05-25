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
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.core.TSDB
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.dao.RollupMetaDao
import org.yupana.externallinks.items.ItemsInvertedIndexImpl
import org.yupana.hbase.{
  ExternalLinkHBaseConnection,
  HBaseUtils,
  InvertedIndexDaoHBase,
  RollupMetaDaoHBase,
  Serializers,
  TSDBHBase
}
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemDimension }

class EtlContext(
    val cfg: EtlConfig,
    schema: Schema,
    metricCollectorCreator: Query => MetricQueryCollector
) extends Serializable {
  def hBaseConfiguration: Configuration = {
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", cfg.hbaseZookeeper)
    hbaseconf.set("zookeeper.session.timeout", 180000.toString)
    cfg.hbaseWriteBufferSize.foreach(x => hbaseconf.set("hbase.client.write.buffer", x.toString))
    hbaseconf
  }

  private def initTsdb: TSDB = {
    val tsdb =
      TSDBHBase(
        hBaseConfiguration,
        cfg.hbaseNamespace,
        schema,
        identity,
        cfg.properties,
        cfg,
        metricCollectorCreator
      )
    setup(tsdb)
    EtlContext.tsdb = Some(tsdb)
    tsdb
  }

  private def initRollupMetaDao: RollupMetaDao = {
    val connection = ConnectionFactory.createConnection(hBaseConfiguration)
    HBaseUtils.checkNamespaceExistsElseCreate(connection, cfg.hbaseNamespace)
    val dao = new RollupMetaDaoHBase(connection, cfg.hbaseNamespace)
    EtlContext.rollupMetaDao = Some(dao)
    dao
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
      schema,
      invertedIndexDao,
      cfg.putIntoInvertedIndex,
      ItemsInvertedIndex
    )
    tsdbInstance.registerExternalLink(ItemsInvertedIndex, itemsInvertedIndex)
  }

  @transient lazy val tsdb: TSDB = EtlContext.tsdb.getOrElse(initTsdb)
  @transient lazy val rollupMetaDao: RollupMetaDao = EtlContext.rollupMetaDao.getOrElse(initRollupMetaDao)
}

object EtlContext {
  private var tsdb: Option[TSDB] = None
  private var rollupMetaDao: Option[RollupMetaDao] = None
}
