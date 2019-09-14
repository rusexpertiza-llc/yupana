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

import org.springframework.jdbc.core.JdbcTemplate
import org.yupana.api.schema.{Dimension, ExternalLink}
import org.yupana.core.TsdbBase
import org.yupana.externallinks.universal.JsonCatalogs.{SQLExternalLinkConnection, SQLExternalLinkDescription}
import org.yupana.externallinks.universal.SQLSourcedExternalLinkService
import org.yupana.schema.{Dimensions, SchemaRegistry}

trait OrganisationCatalog extends ExternalLink{

  val TYPE = "type"
  val ID = "id"

  override val linkName: String = "Organisations"

  override val dimension: Dimension = Dimensions.KKM_ID_TAG

  override val fieldsNames: Set[String] = Set(TYPE, ID)
}

object OrganisationCatalog extends OrganisationCatalog

object OrganisationCatalogImpl {
  val description: SQLExternalLinkDescription = SQLExternalLinkDescription(OrganisationCatalog,
    SchemaRegistry.defaultSchema.tables.map(_._2.name).toSeq,
    Some(Map(
      OrganisationCatalog.TYPE -> "o.type",
      OrganisationCatalog.ID -> "o.org_id",
      Dimensions.KKM_ID_TAG.name -> "k.device_id"
    )),
    Some("kkms k INNER JOIN organisations o on k.org_id = o.id")
  )

  def connection(properties: Properties): SQLExternalLinkConnection = {
    SQLExternalLinkConnection(
      properties.getProperty("orgCatalog.dbUrl"),
      Option(properties.getProperty("orgCatalog.dbUser")),
      Option(properties.getProperty("orgCatalog.dbPassword"))
    )
  }
}

class OrganisationCatalogImpl(tsdb: TsdbBase, jdbcTemplate: JdbcTemplate)
  extends SQLSourcedExternalLinkService(OrganisationCatalog, OrganisationCatalogImpl.description, jdbcTemplate, tsdb)
