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

import com.typesafe.scalalogging.StrictLogging
import org.springframework.jdbc.core.JdbcTemplate
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ DimensionExpr, Expression, InExpr, LinkExpr, NotInExpr }
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.cache.{ Cache, CacheFactory }
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.externallinks.universal.JsonCatalogs.SQLExternalLinkDescription
import org.yupana.schema.externallinks.ExternalLinks._

import scala.collection.JavaConverters._

class SQLSourcedExternalLinkService(
    override val externalLink: ExternalLink,
    config: SQLExternalLinkDescription,
    jdbc: JdbcTemplate
) extends ExternalLinkService[ExternalLink]
    with StrictLogging {

  import SQLSourcedExternalLinkService._
  import config._

  private val mapping = config.fieldsMapping

  override def setLinkedValues(
      exprIndex: collection.Map[Expression, Int],
      rows: Seq[InternalRow],
      exprs: Set[LinkExpr]
  ): Unit = {
    ExternalLinkUtils.setLinkedValues(externalLink, exprIndex, rows, exprs, fieldValuesForDimValues)
  }

  override def condition(condition: Condition): Condition = {
    ExternalLinkUtils.transformCondition(externalLink.linkName, condition, includeCondition, excludeCondition)
  }

  private def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val tagValues = tagValuesForFieldsValues(values, "AND").filter(x => x != null && x.nonEmpty)
    InExpr(DimensionExpr(externalLink.dimension), tagValues)
  }

  private def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val tagValues = tagValuesForFieldsValues(values, "OR").filter(x => x != null && x.nonEmpty)
    NotInExpr(DimensionExpr(externalLink.dimension), tagValues)
  }

  private def catalogFieldToSqlField(cf: FieldName): String = mapping.flatMap(_.get(cf)).getOrElse(camelToSnake(cf))

  private def catalogFieldToSqlFieldWithAlias(cf: FieldName): String =
    s"${catalogFieldToSqlField(cf)} AS ${SQLSourcedExternalLinkService.camelToSnake(cf)}"

  def projection(fields: Set[FieldName]): String = {
    (fields + dimensionName).map(catalogFieldToSqlFieldWithAlias).mkString(", ")
  }

  def relation: String = config.relation.getOrElse(camelToSnake(linkName))

  def fieldValuesForDimValues(
      fields: Set[FieldName],
      tagValues: Set[DimensionValue]
  ): Table[DimensionValue, FieldName, FieldValue] = {

    def withLinkName(dimVal: DimensionValue): String = s"${externalLink.linkName}:$dimVal"
    def withoutLinkName(cacheKey: String): DimensionValue = cacheKey.substring(cacheKey.indexOf(':') + 1)

    val tableRows = fieldValuesForDimValuesCache.getAll(tagValues.map(withLinkName)).map {
      case (k, v) => withoutLinkName(k) -> v
    }
    if (tagValues.forall(tableRows.contains)) {
      SparseTable(tableRows)
    } else {
      val missedKeys = tagValues diff tableRows.keys.toSet
      val q = fieldsByTagsQuery(externalLink.fieldsNames, missedKeys.size)
      val params = missedKeys.map(_.asInstanceOf[Object]).toArray
      logger.debug(s"Query for fields for catalog $linkName: $q with params: $params")
      val dataFromDb = jdbc
        .queryForList(q, params: _*)
        .asScala
        .map(vs =>
          vs.asScala.toMap
            .mapValues(_.toString)
            .map { case (k, v) => snakeToCamel(k) -> v }
        )
        .groupBy(m => m(dimensionName))
        .mapValues(_.head - dimensionName)
      fieldValuesForDimValuesCache.putAll(dataFromDb.map { case (k, v) => withLinkName(k) -> v })
      SparseTable(tableRows ++ dataFromDb)
    }
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

  private def tagValuesForFieldsValues(
      fieldsValues: Seq[(FieldName, Set[FieldValue])],
      joiningOperator: String
  ): Set[DimensionValue] = {
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
      case (acc, c)              => acc + c
    }
  }

  def snakeToCamel(s: String): String = {
    snakeParts.replaceAllIn(s, _.group(1).toUpperCase())
  }

  val fieldValuesForDimValuesCache: Cache[String, Map[FieldName, FieldValue]] =
    CacheFactory.initCache[String, Map[FieldName, FieldValue]]("UniversalCatalogFieldValuesCache")
}
