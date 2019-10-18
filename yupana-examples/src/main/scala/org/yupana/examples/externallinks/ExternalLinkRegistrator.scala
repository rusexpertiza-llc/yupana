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

import java.util.Properties

import com.zaxxer.hikari.{ HikariConfig, HikariDataSource }
import org.apache.hadoop.conf.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.yupana.api.schema.{ ExternalLink, Schema }
import org.yupana.core.TsdbBase
import org.yupana.externallinks.items.{ ItemsInvertedIndexImpl, RelatedItemsCatalogImpl }
import org.yupana.externallinks.universal.JsonCatalogs.{ SQLExternalLink, SQLExternalLinkConnection }
import org.yupana.externallinks.universal.SQLSourcedExternalLinkService
import org.yupana.hbase.{ ExternalLinkHBaseConnection, InvertedIndexDaoHBase }
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }

class ExternalLinkRegistrator(
    tsdb: TsdbBase,
    hbaseConfiguration: Configuration,
    hbaseNamespace: String,
    properties: Properties
) {

  lazy val hBaseConnection = new ExternalLinkHBaseConnection(hbaseConfiguration, hbaseNamespace)

  lazy val invertedDao = new InvertedIndexDaoHBase[String, Long](
    hBaseConnection,
    ItemsInvertedIndexImpl.TABLE_NAME,
    InvertedIndexDaoHBase.stringSerializer,
    InvertedIndexDaoHBase.stringDeserializer,
    InvertedIndexDaoHBase.longSerializer,
    InvertedIndexDaoHBase.longDeserializer
  )

  lazy val invertedIndex = new ItemsInvertedIndexImpl(tsdb, invertedDao, ItemsInvertedIndex)

  def registerExternalLink(link: ExternalLink): Unit = {
    val service = link match {
      case c: SQLExternalLink  => createSqlService(c, tsdb)
      case ItemsInvertedIndex  => invertedIndex
      case RelatedItemsCatalog => new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)
      case AddressCatalog      => new AddressCatalogImpl(tsdb, AddressCatalog)
      case OrganisationCatalog =>
        val jdbcTemplate = createConnection(OrganisationCatalogImpl.connection(properties))
        new OrganisationCatalogImpl(tsdb, jdbcTemplate)
    }

    tsdb.registerExternalLink(link, service)
  }

  def registerAll(schema: Schema): Unit = {
    schema.tables.flatMap {
      case (_, t) => t.externalLinks
    }.toSet foreach registerExternalLink
  }

  def createConnection(config: SQLExternalLinkConnection): JdbcTemplate = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(config.url)
    config.username.foreach(hikariConfig.setUsername)
    config.password.foreach(hikariConfig.setPassword)
    val dataSource = new HikariDataSource(hikariConfig)
    new JdbcTemplate(dataSource)
  }

  def createSqlService(link: SQLExternalLink, tsdb: TsdbBase): SQLSourcedExternalLinkService = {
    val jdbc = createConnection(link.config.connection)
    new SQLSourcedExternalLinkService(link, link.config.description, jdbc, tsdb)
  }
}
