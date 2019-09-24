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

import org.json4s.jackson.JsonMethods
import org.yupana.api.schema.Schema

import scala.collection.mutable.ListBuffer

object JsonExternalLinkDeclarationsParser {

  import JsonCatalogs._
  import org.json4s._

  implicit private val formats: Formats = DefaultFormats

  def parse(schema: Schema, declaration: String): Either[String, Seq[SQLExternalLinkConfig]] = {
    JsonMethods.parse(declaration) \\ "externalLinks" match {
      case jCatalogs: JArray =>
        val (errors, catalogs) = jCatalogs.arr map extractCatalog(schema) partition (_.isLeft)
        if (errors.isEmpty) {
          Right(catalogs.map(_.right.get))
        } else {
          Left(errors.map(_.left.get).mkString(", "))
        }
      case _ => Left(s"No 'externalLinks' array was found in $declaration")
    }
  }

  def extractCatalog(schema: Schema)(jLink: JValue): Either[String, SQLExternalLinkConfig] = {
    jLink \\ "source" match {
      case JString("sql") =>
        try {
          validate(schema, jLink.extract[SQLExternalLinkConfig])
        } catch {
          case e: Exception => Left(s"Can not parse external link ${JsonMethods.compact(jLink)}: ${e.getMessage}")
        }
      case _ => Left(s"Bad source field in ${JsonMethods.compact(jLink)}")
    }
  }

  def validate(schema: Schema, link: SQLExternalLinkConfig): Either[String, SQLExternalLinkConfig] = {

    val errors = ListBuffer.empty[String]

    if (link.description.tables.isEmpty) {
      errors += s"No tables defined for external link ${link.description.linkName}"
    }

    val tablesErrors = link.description.tables map { s =>
      (s, schema.getTable(s))
    } collect {
      case (s, None) => s"Unknown table: $s in '${link.description.linkName}'"
    }

    link.description.fieldsMapping map { fm =>
      val mappedFieldsSet = fm.keys.toSet
      if (mappedFieldsSet != (link.description.fieldsNames + link.description.dimensionName)) {
        errors += s"Fields mapping keys set is not equal to declared external link fields set: " +
          s"$mappedFieldsSet != ${link.description.fieldsNames + link.description.dimensionName} in '${link.description.linkName}'"
      }
      val sqlFieldNames = fm.values.toList
      if (sqlFieldNames.distinct.size != sqlFieldNames.size) {
        errors += s"Inverse fields mapping contains duplicated keys: $sqlFieldNames in '${link.description.linkName}'"
      }
    } getOrElse Seq.empty

    val allErrors = errors ++ tablesErrors

    if (allErrors.isEmpty) Right(link) else Left(allErrors.mkString(", "))
  }

}
