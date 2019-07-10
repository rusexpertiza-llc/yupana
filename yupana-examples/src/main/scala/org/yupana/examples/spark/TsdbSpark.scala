package org.yupana.examples.spark

import org.apache.spark.SparkContext
import org.yupana.api.query.Query
import org.yupana.api.schema.{ExternalLink, Schema}
import org.yupana.core.ExternalLinkService
import org.yupana.core.cache.CacheFactory
import org.yupana.examples.externallinks.ExternalLinkRegistrator
import org.yupana.spark.{Config, TsDaoHBaseSpark, TsdbSparkBase}

object TsdbSpark {
  var externalLinks = Map.empty[String, ExternalLinkService[_ <: ExternalLink]]
}

class TsdbSpark(sparkContext: SparkContext,
                prepareQuery: Query => Query,
                conf: Config,
                schema: Schema
               ) extends TsdbSparkBase(sparkContext, prepareQuery, conf, schema) {
  @transient lazy val elRegistrator =
    new ExternalLinkRegistrator(this, TsDaoHBaseSpark.hbaseConfiguration(conf), conf.hbaseNamespace, conf.properties)

  override def linkService(el: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    if (!TsdbSpark.externalLinks.contains(el.linkName)) {
      logger.info(s"${el.linkName} is not registered yet, so let's create and register it")
      CacheFactory.init(conf.properties, conf.hbaseNamespace)
      elRegistrator.registerExternalLink(el)
    }
    TsdbSpark.externalLinks.getOrElse(el.linkName,
      throw new Exception(s"Can't find catalog ${el.linkName}: ${el.fieldsNames}"))
  }

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit = {
    TsdbSpark.externalLinks += (catalog.linkName -> catalogService)
  }
}
