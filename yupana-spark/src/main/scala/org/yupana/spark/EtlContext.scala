package org.yupana.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.yupana.api.schema.Schema
import org.yupana.core.TSDB
import org.yupana.externallinks.items.ItemsInvertedIndexImpl
import org.yupana.hbase.{ExternalLinkHBaseConnection, InvertedIndexDaoHBase, TSDBHbase}
import org.yupana.schema.externallinks.ItemsInvertedIndex

class EtlContext(val cfg: EtlConfig, schema: Schema) extends Serializable {
  def hBaseConfiguration: Configuration = {
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", cfg.hbaseZookeeper)
    hbaseconf.set("zookeeper.session.timeout", 180000.toString)
    cfg.hbaseWriteBufferSize.foreach(x => hbaseconf.set("hbase.client.write.buffer", x.toString))
    hbaseconf
  }

  private def init: (TSDB, ItemsInvertedIndexImpl) = {
    val tsdb = TSDBHbase(hBaseConfiguration, cfg.hbaseNamespace, schema, identity, cfg.properties)
    val hBaseConnection = new ExternalLinkHBaseConnection(hBaseConfiguration, cfg.hbaseNamespace)
    val invertedIndexDao = new InvertedIndexDaoHBase[String, Long](
      hBaseConnection,
      ItemsInvertedIndexImpl.TABLE_NAME,
      InvertedIndexDaoHBase.stringSerializer,
      InvertedIndexDaoHBase.longSerializer,
      InvertedIndexDaoHBase.longDeserializer
    )
    val itemsInvertedIndex = new ItemsInvertedIndexImpl(tsdb, invertedIndexDao, ItemsInvertedIndex)
    tsdb.registerExternalLink(ItemsInvertedIndex, itemsInvertedIndex)
    EtlContext.tsdb = Some(tsdb)
    EtlContext.itemsInvertedIndex = Some(itemsInvertedIndex)
    (tsdb, itemsInvertedIndex)
  }

  @transient lazy val tsdb: TSDB = EtlContext.tsdb.getOrElse(init._1)
  @transient lazy val itemsInvertedIndex: ItemsInvertedIndexImpl = EtlContext.itemsInvertedIndex.getOrElse(init._2)
}

object EtlContext {
  private var tsdb: Option[TSDB] =  None
  private var itemsInvertedIndex: Option[ItemsInvertedIndexImpl] = None
}
