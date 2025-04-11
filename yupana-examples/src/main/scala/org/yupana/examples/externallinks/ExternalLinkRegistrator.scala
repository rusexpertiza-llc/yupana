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

package org.yupana.examples.externallinks

import com.zaxxer.hikari.{ HikariConfig, HikariDataSource }

import javax.sql.DataSource
import org.apache.hadoop.conf.Configuration
import org.yupana.api.schema.{ ExternalLink, Schema }
import org.yupana.core.TsdbBase
import org.yupana.externallinks.items.{ ItemsInvertedIndexImpl, RelatedItemsCatalogImpl }
import org.yupana.externallinks.universal.JsonCatalogs.{ SQLExternalLink, SQLExternalLinkConnection }
import org.yupana.externallinks.universal.SQLSourcedExternalLinkService
import org.yupana.hbase.{ ExternalLinkHBaseConnection, InvertedIndexDaoHBase, Serializers }
import org.yupana.schema.ItemDimension
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.settings.Settings

class ExternalLinkRegistrator(
    tsdb: TsdbBase,
    hbaseConfiguration: Configuration,
    hbaseNamespace: String,
    settings: Settings
) {

  lazy val hBaseConnection = new ExternalLinkHBaseConnection(hbaseConfiguration, hbaseNamespace)

  lazy val invertedDao = new InvertedIndexDaoHBase[String, ItemDimension.KeyType](
    hBaseConnection,
    ItemsInvertedIndexImpl.TABLE_NAME,
    Serializers.stringSerializer,
    Serializers.stringDeserializer,
    ItemsInvertedIndexImpl.valueSerializer,
    ItemsInvertedIndexImpl.valueDeserializer
  )

  lazy val invertedIndex =
    new ItemsInvertedIndexImpl(
      tsdb.schema,
      invertedDao,
      putEnabled = false,
      externalLink = ItemsInvertedIndex
    )

  def registerExternalLink(link: ExternalLink): Unit = {
    val service = link match {
      case c: SQLExternalLink[_] => createSqlService(c)
      case ItemsInvertedIndex    => invertedIndex
      case RelatedItemsCatalog   => new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)
      case AddressCatalog        => new AddressCatalogImpl(tsdb.schema, AddressCatalog)
      case OrganisationCatalog =>
        val dataSource = createConnection(OrganisationCatalogImpl.connection(settings))
        new OrganisationCatalogImpl(tsdb.schema, dataSource)
      case _ => throw new IllegalArgumentException(s"Unknown external link ${link.linkName}")
    }

    tsdb.registerExternalLink(link, service)
  }

  def registerAll(schema: Schema): Unit = {
    schema.tables.flatMap {
      case (_, t) => t.externalLinks
    }.toSet foreach registerExternalLink
  }

  def createConnection(config: SQLExternalLinkConnection): DataSource = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(config.url)
    config.username.foreach(hikariConfig.setUsername)
    config.password.foreach(hikariConfig.setPassword)
    new HikariDataSource(hikariConfig)
  }

  def createSqlService(link: SQLExternalLink[_]): SQLSourcedExternalLinkService[link.DimType] = {
    val jdbc = createConnection(link.config.connection)
    new SQLSourcedExternalLinkService[link.DimType](tsdb.schema, link, link.config.description, jdbc)
  }
}
