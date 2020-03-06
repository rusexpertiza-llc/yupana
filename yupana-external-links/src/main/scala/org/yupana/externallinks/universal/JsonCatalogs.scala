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

package org.yupana.externallinks.universal

import org.yupana.api.schema.{ Dimension, ExternalLink, LinkMetric, Schema }
import org.yupana.schema.externallinks.ExternalLinks.FieldName

object JsonCatalogs {

  case class SQLExternalLinkConfig(description: SQLExternalLinkDescription, connection: SQLExternalLinkConnection)

  case class SQLExternalLinkDescription(
      linkName: String,
      dimensionName: String,
      fieldsNames: Set[String],
      tables: Seq[String],
      fieldsMapping: Option[Map[FieldName, String]],
      relation: Option[String]
  )

  case class SQLExternalLinkConnection(url: String, username: Option[String], password: Option[String])

  object SQLExternalLinkDescription {
    def apply(
        externalLink: ExternalLink,
        tables: Seq[String],
        fieldsMapping: Option[Map[FieldName, String]],
        relation: Option[String]
    ): SQLExternalLinkDescription = {
      new SQLExternalLinkDescription(
        externalLink.linkName,
        externalLink.dimension.name,
        externalLink.fieldsNames.map(_.name),
        tables,
        fieldsMapping,
        relation
      )
    }
  }

  case class SQLExternalLink(config: SQLExternalLinkConfig, dimension: Dimension) extends ExternalLink {
    override val linkName: String = config.description.linkName
    override val fieldsNames: Set[LinkMetric] = config.description.fieldsNames map LinkMetric[String]
  }

  def attachLinkToSchema(schema: Schema, config: SQLExternalLinkConfig): Schema = {
    val tables = config.description.tables.flatMap(schema.getTable)
    tables.flatMap(_.dimensionSeq.find(_.name == config.description.dimensionName)).headOption match {
      case Some(dim) =>
        val link = SQLExternalLink(config, dim)
        config.description.tables.foldLeft(schema) { (ss, tableName) =>
          ss.withTableUpdated(tableName)(_.withExternalLinks(Seq(link)))
        }
      case None =>
        schema
    }
  }

  def attachLinksToSchema(schema: Schema, configs: Seq[SQLExternalLinkConfig]): Schema = {
    configs.foldLeft(schema)(attachLinkToSchema)
  }
}
