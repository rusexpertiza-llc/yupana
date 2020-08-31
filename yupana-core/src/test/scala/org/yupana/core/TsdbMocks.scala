package org.yupana.core

import org.scalamock.scalatest.MockFactory
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl, TsdbQueryMetricsDao }
import org.yupana.core.model.InternalRow
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
import org.yupana.core.utils.ConditionMatchers.{ EqString, InString, NeqString, NotInString }
import org.yupana.core.utils.Table

trait TsdbMocks extends MockFactory {

  def mockCatalogService(tsdb: TSDB, catalog: ExternalLink): ExternalLinkService[TestLinks.TestLink] = {
    val catalogService = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(catalog, catalogService)

    val externalLink = new TestLinks.TestLink
    (catalogService.externalLink _)
      .expects()
      .returning(externalLink)
      .anyNumberOfTimes()

    (catalogService.isSupportedCondition _)
      .stubs(*)
      .onCall((condition: Condition) =>
        condition match {
          case EqExpr(LinkExpr(c, _), ConstantExpr(_))                        => true
          case NeqExpr(LinkExpr(c, _), ConstantExpr(_))                       => true
          case InExpr(LinkExpr(c, _), _) if c.linkName == catalog.linkName    => true
          case NotInExpr(LinkExpr(c, _), _) if c.linkName == catalog.linkName => true
          case _                                                              => false
        }
      )

    catalogService
  }

  def withTsdbMock(body: (TSDB, TSTestDao) => Unit): Unit = {
    val tsdbDaoMock = mock[TSTestDao]
    val metricsDaoMock = mock[TsdbQueryMetricsDao]
    (tsdbDaoMock.isSupportedCondition _)
      .expects(*)
      .onCall((c: Condition) =>
        c match {
          case EqExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
          case EqExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
          case NeqExpr(_: TimeExpr.type, ConstantExpr(_))                 => true
          case NeqExpr(ConstantExpr(_), _: TimeExpr.type)                 => true
          case GtExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
          case GtExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
          case LtExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
          case LtExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
          case GeExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
          case GeExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
          case LeExpr(_: TimeExpr.type, ConstantExpr(_))                  => true
          case LeExpr(ConstantExpr(_), _: TimeExpr.type)                  => true
          case _: DimIdInExpr[_, _]                                       => true
          case _: DimIdNotInExpr[_, _]                                    => true
          case EqExpr(_: DimensionExpr[_], ConstantExpr(_))               => true
          case EqExpr(ConstantExpr(_), _: DimensionExpr[_])               => true
          case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_))  => true
          case EqString(ConstantExpr(_), LowerExpr(_: DimensionExpr[_]))  => true
          case NeqExpr(_: DimensionExpr[_], ConstantExpr(_))              => true
          case NeqExpr(ConstantExpr(_), _: DimensionExpr[_])              => true
          case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_)) => true
          case NeqString(LowerExpr(ConstantExpr(_)), _: DimensionExpr[_]) => true
          case InExpr(_: DimensionExpr[_], _)                             => true
          case NotInExpr(_: DimensionExpr[_], _)                          => true
          case InString(LowerExpr(_: DimensionExpr[_]), _)                => true
          case NotInString(LowerExpr(_: DimensionExpr[_]), _)             => true
          case _                                                          => false
        }
      )
      .anyNumberOfTimes()
    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val tsdb = new TSDB(tsdbDaoMock, metricsDaoMock, dictionaryProvider, identity, SimpleTsdbConfig())
    body(tsdb, tsdbDaoMock)
  }

  def setCatalogValueByTag(
      exprIndex: scala.collection.Map[Expression, Int],
      datas: Seq[InternalRow],
      catalog: ExternalLink,
      catalogValues: Table[String, String, String]
  ): Unit = {
    datas.foreach { v =>
      val tagValue = v.get[String](exprIndex, DimensionExpr(catalog.dimension))
      catalogValues.row(tagValue).foreach {
        case (field, value) =>
          v.set(exprIndex, LinkExpr(catalog, field), value)
      }
    }
  }

  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)

  def createQuery(sql: String): Query = {
    SqlParser
      .parse(sql)
      .right
      .flatMap {
        case s: Select => sqlQueryProcessor.createQuery(s)
        case x         => Left(s"SELECT statement expected, but got $x")
      }
      .right
      .map(QueryOptimizer.optimize)
      .fold(fail(_), identity)
  }
}
