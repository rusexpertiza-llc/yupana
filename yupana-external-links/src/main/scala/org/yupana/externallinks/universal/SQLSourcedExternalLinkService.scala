package org.yupana.externallinks.universal

import com.typesafe.scalalogging.StrictLogging
import org.springframework.jdbc.core.JdbcTemplate
import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.CollectionUtils
import org.yupana.core.TsdbBase
import org.yupana.core.utils.{SparseTable, Table}
import org.yupana.externallinks.DimValueBasedExternalLinkService
import org.yupana.externallinks.universal.JsonCatalogs.SQLExternalLinkDescription
import org.yupana.schema.externallinks.ExternalLinks._

import scala.collection.JavaConverters._

class SQLSourcedExternalLinkService(override val externalLink: ExternalLink,
                                    config: SQLExternalLinkDescription,
                                    jdbc: JdbcTemplate,
                                    tsdb: TsdbBase
  ) extends DimValueBasedExternalLinkService[ExternalLink](tsdb) with StrictLogging {

  import SQLSourcedExternalLinkService._
  import config._

  private val mapping = config.fieldsMapping
  private val inverseMapping = mapping.map(_.map(_.swap))

  def catalogFieldToSqlField(cf: FieldName): String = mapping.flatMap(_.get(cf)).getOrElse(camelToSnake(cf))
  def catalogFieldToSqlFieldWithAlias(cf: FieldName): String = s"${catalogFieldToSqlField(cf)} AS ${SQLSourcedExternalLinkService.camelToSnake(cf)}"
  def sqlFieldToCatalogField(sf: FieldName): String = inverseMapping.flatMap(_.get(sf)).getOrElse(snakeToCamel(sf))

  def projection(fields: Set[FieldName]): String = {
    (fields + dimensionName).map(catalogFieldToSqlFieldWithAlias).mkString(", ")
  }

  def relation: String = config.relation.getOrElse(camelToSnake(linkName))

  override def fieldValuesForDimValues(fields: Set[FieldName],
                                       tagValues: Set[DimensionValue]
                                       ): Table[DimensionValue, FieldName, FieldValue] = {
    val q = fieldsByTagsQuery(fields, tagValues.size)
    val params = tagValues.map(_.asInstanceOf[Object]).toArray
    logger.trace(s"Query for fields for catalog $linkName: ${q.take(1000)} with params: ${CollectionUtils.mkStringWithLimit(params)}")
    val dataFromDb = jdbc.queryForList(q, params:_*).asScala
      .map(vs => vs.asScala.toMap.mapValues(_.toString)
        .map { case (k, v) => snakeToCamel(k) -> v})
    SparseTable(dataFromDb.groupBy(m => m(dimensionName)).mapValues(_.head - dimensionName))
  }

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(FieldName, Set[FieldValue])]): Set[DimensionValue] = {
    tagValuesForFieldsValues(fieldsValues, "AND")
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(FieldName, Set[FieldValue])]): Set[DimensionValue] = {
    tagValuesForFieldsValues(fieldsValues, "OR")
  }

  private def fieldsByTagsQuery(fields: Set[FieldName], tagValuesCount: Int): String = {
    s"""SELECT ${projection(fields)}
       |FROM $relation
       |WHERE ${catalogFieldToSqlField(dimensionName)} ${tagValueInClause(tagValuesCount)}""".stripMargin
  }

  private def tagsByFieldsQuery(fieldValues: Seq[(FieldName, Set[FieldValue])], joiningOperator: String): String = {
    s"""SELECT ${catalogFieldToSqlField(dimensionName)}
       |FROM $relation
       |WHERE ${fieldValuesInClauses(fieldValues).mkString(s" $joiningOperator ")}""".stripMargin
  }

  private def tagValueInClause(tagValuesCount: Int): String = {
    s"""IN (${Seq.fill(tagValuesCount)("?").mkString(", ")})"""
  }

  private def fieldValuesInClauses(fieldValues: Seq[(FieldName, Set[FieldValue])]): Seq[String] = {
    fieldValues map {
      case (fieldName, possibleValues) =>
        s"""${catalogFieldToSqlField(fieldName)} IN (${Seq.fill(possibleValues.size)("?").mkString(", ")})"""
    }
  }

  private def tagValuesForFieldsValues(fieldsValues: Seq[(FieldName, Set[FieldValue])],
                                       joiningOperator: String): Set[DimensionValue] = {
    val q = tagsByFieldsQuery(fieldsValues, joiningOperator)
    val params = fieldsValues.flatMap(_._2).map(_.asInstanceOf[Object]).toArray
    logger.debug(s"Query for fields for catalog $linkName: $q with params: $params")
    jdbc.queryForList(q, params, classOf[DimensionValue]).asScala.toSet
  }

}

object SQLSourcedExternalLinkService {

  private val snakeParts = "_([a-z\\d])".r

  def camelToSnake(s: String): String = {
    s.drop(1).foldLeft(s.headOption.map(_.toLower.toString) getOrElse "") {
      case (acc, c) if c.isUpper => acc + "_" + c.toLower
      case (acc, c) => acc + c
    }
  }

  def snakeToCamel(s: String): String = {
    snakeParts.replaceAllIn(s, _.group(1).toUpperCase())
  }
}
