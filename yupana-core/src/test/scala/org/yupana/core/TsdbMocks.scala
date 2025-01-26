package org.yupana.core

import org.scalamock.scalatest.MockFactory
import org.scalatest.TestSuite
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.api.types.{ SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.core.model.BatchDataset
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
import org.yupana.core.utils.Table
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.utils.RussianTokenizer

trait TSTestDao extends TSDao[Iterator, Long]

trait TsdbMocks extends MockFactory { self: TestSuite =>

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
          case EqTime(_: TimeExpr.type, ConstantExpr(_))                  => true
          case EqTime(ConstantExpr(_), _: TimeExpr.type)                  => true
          case NeqTime(_: TimeExpr.type, ConstantExpr(_))                 => true
          case NeqTime(ConstantExpr(_), _: TimeExpr.type)                 => true
          case GtTime(_: TimeExpr.type, ConstantExpr(_))                  => true
          case GtTime(ConstantExpr(_), _: TimeExpr.type)                  => true
          case LtTime(_: TimeExpr.type, ConstantExpr(_))                  => true
          case LtTime(ConstantExpr(_), _: TimeExpr.type)                  => true
          case GeTime(_: TimeExpr.type, ConstantExpr(_))                  => true
          case GeTime(ConstantExpr(_), _: TimeExpr.type)                  => true
          case LeTime(_: TimeExpr.type, ConstantExpr(_))                  => true
          case LeTime(ConstantExpr(_), _: TimeExpr.type)                  => true
          case _: DimIdInExpr[_]                                          => true
          case _: DimIdNotInExpr[_]                                       => true
          case EqExpr(_: DimensionExpr[_], ConstantExpr(_))               => true
          case EqExpr(ConstantExpr(_), _: DimensionExpr[_])               => true
          case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_))  => true
          case EqString(ConstantExpr(_), LowerExpr(_: DimensionExpr[_]))  => true
          case NeqExpr(_: DimensionExpr[_], ConstantExpr(_))              => true
          case NeqExpr(ConstantExpr(_), _: DimensionExpr[_])              => true
          case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_)) => true
          case NeqString(LowerExpr(ConstantExpr(_)), _: DimensionExpr[_]) => true
          case EqString(DimensionIdExpr(_), ConstantExpr(_))              => true
          case EqString(ConstantExpr(_), DimensionIdExpr(_))              => true
          case InExpr(_: DimensionExpr[_], _)                             => true
          case NotInExpr(_: DimensionExpr[_], _)                          => true
          case InString(LowerExpr(_: DimensionExpr[_]), _)                => true
          case NotInString(LowerExpr(_: DimensionExpr[_]), _)             => true
          case _                                                          => false
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
        { (_: Query, _: String) => NoMetricCollector }
      )
    body(tsdb, tsdbDaoMock)
  }

  def setCatalogValueByTag(
      dataset: BatchDataset,
      catalog: ExternalLink,
      catalogValues: Table[String, String, String]
  ): Unit = {
    for (i <- 0 until dataset.size) {
      if (dataset.isDefined(i, DimensionExpr(catalog.dimension))) {
        val dimValue = dataset.get(i, DimensionExpr(catalog.dimension)).asInstanceOf[String]
        catalogValues.row(dimValue).foreach {
          case (field, value) =>
            if (value != null) {
              dataset.set(i, LinkExpr(catalog, field), value)
            }
        }
      }
    }
  }

  implicit val srw: StringReaderWriter = SimpleStringReaderWriter
  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)
  private val calculator = new ConstantCalculator(RussianTokenizer)

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
