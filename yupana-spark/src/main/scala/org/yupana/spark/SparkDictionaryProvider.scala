package org.yupana.spark

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.yupana.api.schema.Dimension
import org.yupana.core.Dictionary
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{DictionaryDao, DictionaryProvider}
import org.yupana.hbase.DictionaryDaoHBase

class SparkDictionaryProvider(config: Config) extends DictionaryProvider with Serializable {
  override def dictionary(dimension: Dimension): Dictionary = {
    SparkDictionaryProvider.dictionaries.get(dimension) match {
      case Some(d) => d
      case None =>
        CacheFactory.init(config.properties, config.hbaseNamespace)
        val d = new Dictionary(dimension, localDictionaryDao)
        SparkDictionaryProvider.dictionaries += dimension -> d
        d
    }
  }

  @transient private lazy val localDictionaryDao: DictionaryDao = {
    SparkDictionaryProvider.dictionaryDao match {
      case Some(dao) => dao
      case None =>
        val dao = new DictionaryDaoHBase(ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(config)), config.hbaseNamespace)
        SparkDictionaryProvider.dictionaryDao = Some(dao)
        dao
    }
  }
}

object SparkDictionaryProvider {
  var dictionaryDao = Option.empty[DictionaryDao]
  var dictionaries = Map.empty[Dimension, Dictionary]
}
