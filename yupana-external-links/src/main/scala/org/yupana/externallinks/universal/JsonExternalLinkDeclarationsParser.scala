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

import cats.data.Validated
import cats.data.Validated.{ Invalid, Valid }
import cats.implicits._
import io.circe.CursorOp.DownField
import io.circe._
import io.circe.generic.auto._
import org.yupana.api.schema.Schema

import scala.collection.mutable.ListBuffer

object JsonExternalLinkDeclarationsParser {

  import JsonCatalogs._

  val configDecoder: Decoder[SQLExternalLinkConfig] = Decoder[SQLExternalLinkConfig]
    .validate(_.downField("description").downField("source").as[String].contains("sql"), "bad source field")

  def parse(schema: Schema, declaration: String): Either[String, Seq[SQLExternalLinkConfig]] = {
    val json = parser.parse(declaration).getOrElse(Json.Null)

    json.hcursor
      .downField("externalLinks")
      .as[Seq[Json]]
      .left
      .map(_ => s"No 'externalLinks' array was found in ${json.noSpaces}")
      .flatMap {
        _.map(linkJson => parseConfig(schema, linkJson).toValidatedNel).toList.sequence.toEither.left
          .map(_.mkString_(". "))
      }
  }

  private def parseConfig(schema: Schema, json: Json): Validated[String, SQLExternalLinkConfig] = {
    configDecoder.tryDecodeAccumulating(json.hcursor) match {
      case Valid(c) => validate(schema, c).toValidated

      case Invalid(nel) =>
        val linkId = json.hcursor
          .downField("description")
          .downField("linkName")
          .as[String]
          .map(n => s"'$n'")
          .getOrElse(json.noSpaces)

        val prefix = s"Can not parse external link $linkId: "
        val msgs = nel.map { f =>
          val c = json.hcursor.replay(f.history)
          if (c.failed) {
            val fields = c.history.collect { case DownField(name) => name }
            if (fields.nonEmpty) s"no usable value for ${fields.reverse.mkString(".")}"
            else f.message
          } else f.message
        }

        (prefix + msgs.mkString_(", ")).invalid
    }
  }

  private def validate(schema: Schema, link: SQLExternalLinkConfig): Either[String, SQLExternalLinkConfig] = {
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
