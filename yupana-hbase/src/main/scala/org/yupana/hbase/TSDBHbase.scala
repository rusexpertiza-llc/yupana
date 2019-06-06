package org.yupana.hbase

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.core.TSDB
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.DictionaryProviderImpl

object TSDBHbase {
  def apply(config: Configuration, namespace: String, schema: Schema, prepareQuery: Query => Query, properties: Properties): TSDB = {
    val connection = ConnectionFactory.createConnection(config)
    HBaseUtils.initStorage(connection, namespace, schema)

    CacheFactory.init(properties, namespace)

    val collectMetrics = Option(properties.getProperty("analytics.tsdb.collect-metrics")).exists(_.toBoolean)
    val extractBatchSize = Option(properties.getProperty("analytics.tsdb.extract-batch-size")).map(_.toInt).getOrElse(10000)
    val putsBatchSize = Option(properties.getProperty("analytics.tsdb.put-batch-size")).map(_.toInt).getOrElse(1000)

    val dictDao = new DictionaryDaoHBase(connection, namespace)
    val dictProvider = new DictionaryProviderImpl(dictDao)
    val dao = new TSDaoHBase(connection, namespace, dictProvider, putsBatchSize)
    new TSDB(dao, dictProvider, prepareQuery, extractBatchSize, collectMetrics)
  }
}
