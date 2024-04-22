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

package org.yupana.core.sql.parser

sealed trait Statement

case class Select(
    tableName: Option[String],
    fields: SqlFields,
    condition: Option[SqlExpr],
    groupings: Seq[SqlExpr],
    having: Option[SqlExpr],
    limit: Option[Int]
) extends Statement

case class Upsert(
    tableName: String,
    fieldNames: Seq[String],
    values: Seq[Seq[SqlExpr]]
) extends Statement

case object ShowTables extends Statement

case object ShowVersion extends Statement

case object ShowUsers extends Statement

case class ShowColumns(table: String) extends Statement

case class MetricsFilter(queryId: Option[String] = None, state: Option[String] = None)

case class ShowQueryMetrics(filter: Option[MetricsFilter], limit: Option[Int]) extends Statement

case class KillQuery(filter: MetricsFilter) extends Statement

case class DeleteQueryMetrics(filter: MetricsFilter) extends Statement

case class ShowFunctions(dataType: String) extends Statement

case class ShowUpdatesIntervals(condition: Option[SqlExpr]) extends Statement

case class CreateUser(name: String, password: Option[String], role: Option[String]) extends Statement
case class AlterUser(name: String, password: Option[String], role: Option[String]) extends Statement
case class DropUser(name: String) extends Statement

case class SetValue(key: String, value: Value) extends Statement
