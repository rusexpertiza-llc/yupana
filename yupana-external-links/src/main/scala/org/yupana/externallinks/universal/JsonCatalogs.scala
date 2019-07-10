package org.yupana.externallinks.universal

import org.yupana.api.schema.{Dimension, ExternalLink, Schema}
import org.yupana.schema.externallinks.ExternalLinks.FieldName

object JsonCatalogs {

  case class SQLExternalLinkConfig(description: SQLExternalLinkDescription,
                                   connection: SQLExternalLinkConnection
                                  )

  case class SQLExternalLinkDescription(linkName: String,
                                        dimensionName: String,
                                        fieldsNames: Set[String],
                                        tables: Seq[String],
                                        fieldsMapping: Option[Map[FieldName, String]],
                                        relation: Option[String]
  )

  case class SQLExternalLinkConnection(url: String,
                                       username: Option[String],
                                       password: Option[String]
                                      )

  object SQLExternalLinkDescription {
    def apply(externalLink: ExternalLink,
              tables: Seq[String],
              fieldsMapping: Option[Map[FieldName, String]],
              relation: Option[String]
             ): SQLExternalLinkDescription = {
      new SQLExternalLinkDescription(
        externalLink.linkName,
        externalLink.dimension.name,
        externalLink.fieldsNames,
        tables,
        fieldsMapping,
        relation
      )
    }
  }

  case class SQLExternalLink(config: SQLExternalLinkConfig, dimension: Dimension) extends ExternalLink {
    override val linkName: String = config.description.linkName
    override val fieldsNames: Set[String] = config.description.fieldsNames
  }

  def attachLinkToSchema(schema: Schema, config: SQLExternalLinkConfig): Schema = {
    val tables = config.description.tables.flatMap(schema.getTable)
    tables.flatMap(_.dimensionSeq.find(_.name == config.description.dimensionName)).headOption match {
      case Some(dim) =>
        val link = SQLExternalLink(config, dim)
        config.description.tables.foldLeft(schema) {
          (ss, tableName) => ss.withTableUpdated(tableName)(_.withExternalLinks(Seq(link)))
        }
      case None =>
        schema
    }
  }

  def attachLinksToSchema(schema: Schema, configs: Seq[SQLExternalLinkConfig]): Schema = {
    configs.foldLeft(schema)(attachLinkToSchema)
  }
}
