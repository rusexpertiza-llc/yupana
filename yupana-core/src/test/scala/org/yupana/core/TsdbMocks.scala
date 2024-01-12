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

package org.yupana.core

import org.scalamock.scalatest.MockFactory
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
import org.yupana.core.utils.Table
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.utils.RussianTokenizer

trait TSTestDao extends TSDao[Iterator, Long]

trait TsdbMocks extends MockFactory {

  def mockCatalogService(tsdb: TSDB, catalog: ExternalLink): ExternalLinkService[TestLinks.TestLink] = {
    val catalogService = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(catalog, catalogService)

    val externalLink = new TestLinks.TestLink
    (() => catalogService.externalLink)
      .expects()
      .returning(externalLink)
      .anyNumberOfTimes()

    catalogService
  }

  def daoMock: TSTestDao = {
    val tsdbDaoMock = mock[TSTestDao]
    (tsdbDaoMock.isSupportedCondition _)
      .expects(*)
      .onCall((c: Condition) =>
        c match {
          case EqTime(_: TimeExpr.type, ConstantExpr(_, _))                  => true
          case EqTime(ConstantExpr(_, _), _: TimeExpr.type)                  => true
          case NeqTime(_: TimeExpr.type, ConstantExpr(_, _))                 => true
          case NeqTime(ConstantExpr(_, _), _: TimeExpr.type)                 => true
          case GtTime(_: TimeExpr.type, ConstantExpr(_, _))                  => true
          case GtTime(ConstantExpr(_, _), _: TimeExpr.type)                  => true
          case LtTime(_: TimeExpr.type, ConstantExpr(_, _))                  => true
          case LtTime(ConstantExpr(_, _), _: TimeExpr.type)                  => true
          case GeTime(_: TimeExpr.type, ConstantExpr(_, _))                  => true
          case GeTime(ConstantExpr(_, _), _: TimeExpr.type)                  => true
          case LeTime(_: TimeExpr.type, ConstantExpr(_, _))                  => true
          case LeTime(ConstantExpr(_, _), _: TimeExpr.type)                  => true
          case _: DimIdInExpr[_, _]                                          => true
          case _: DimIdNotInExpr[_, _]                                       => true
          case EqExpr(_: DimensionExpr[_], ConstantExpr(_, _))               => true
          case EqExpr(ConstantExpr(_, _), _: DimensionExpr[_])               => true
          case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_, _))  => true
          case EqString(ConstantExpr(_, _), LowerExpr(_: DimensionExpr[_]))  => true
          case NeqExpr(_: DimensionExpr[_], ConstantExpr(_, _))              => true
          case NeqExpr(ConstantExpr(_, _), _: DimensionExpr[_])              => true
          case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_, _)) => true
          case NeqString(LowerExpr(ConstantExpr(_, _)), _: DimensionExpr[_]) => true
          case EqString(DimensionIdExpr(_), ConstantExpr(_, _))              => true
          case EqString(ConstantExpr(_, _), DimensionIdExpr(_))              => true
          case InExpr(_: DimensionExpr[_], _)                                => true
          case NotInExpr(_: DimensionExpr[_], _)                             => true
          case InString(LowerExpr(_: DimensionExpr[_]), _)                   => true
          case NotInString(LowerExpr(_: DimensionExpr[_]), _)                => true
          case _                                                             => false
        }
      )
      .anyNumberOfTimes()

    (tsdbDaoMock.mapReduceEngine _)
      .expects(*)
      .onCall((_: MetricQueryCollector) => IteratorMapReducible.iteratorMR)
      .anyNumberOfTimes()

    tsdbDaoMock
  }

  def withTsdbMock(body: (TSDB, TSTestDao) => Unit): Unit = {
    val tsdbDaoMock = daoMock
    val changelogDaoMock = mock[ChangelogDao]
    val tsdb =
      new TSDB(
        TestSchema.schema,
        tsdbDaoMock,
        changelogDaoMock,
        identity,
        SimpleTsdbConfig(),
        { _: Query => NoMetricCollector }
      )
    body(tsdb, tsdbDaoMock)
  }

  def setCatalogValueByTag(
      builder: InternalRowBuilder,
      rows: Seq[InternalRow],
      catalog: ExternalLink,
      catalogValues: Table[String, String, String]
  ): Seq[InternalRow] = {
    rows.map { row =>
      builder.setFieldsFromRow(row)
      val tagValue = row.get(builder, DimensionExpr(catalog.dimension)).asInstanceOf[String]
      catalogValues.row(tagValue).foreach {
        case (field, value) =>
          builder.set(LinkExpr(catalog, field), value)
      }
      builder.buildAndReset()
    }
  }

  private val calculator = new ConstantCalculator(RussianTokenizer)

  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)

  def createQuery(sql: String): Query = {
    SqlParser
      .parse(sql)
      .flatMap {
        case s: Select => sqlQueryProcessor.createQuery(s)
        case x         => Left(s"SELECT statement expected, but got $x")
      }
      .map(QueryOptimizer.optimize(calculator))
      .fold(fail(_), identity)
  }
}
