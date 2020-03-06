package org.yupana.core

import org.scalamock.scalatest.MockFactory
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl, TSDao, TsdbQueryMetricsDao }
import org.yupana.core.model.InternalRow
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
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
          case BinaryOperationExpr(op, LinkExpr(c, _), ConstantExpr(_))
              if Set("==", "!=").contains(op.name) && c.linkName == catalog.linkName =>
            true
          case InExpr(LinkExpr(c, _), _) if c.linkName == catalog.linkName    => true
          case NotInExpr(LinkExpr(c, _), _) if c.linkName == catalog.linkName => true
          case _                                                              => false
        }
      )

    catalogService
  }

  trait TSTestDao extends TSDao[Iterator, Long]
  def withTsdbMock(body: (TSDB, TSTestDao) => Unit): Unit = {
    val tsdbDaoMock = mock[TSTestDao]
    val metricsDaoMock = mock[TsdbQueryMetricsDao]
    (tsdbDaoMock.isSupportedCondition _)
      .expects(*)
      .onCall((c: Condition) =>
        c match {
          case BinaryOperationExpr(_, _: TimeExpr.type, ConstantExpr(_)) => true
          case BinaryOperationExpr(_, ConstantExpr(_), _: TimeExpr.type) => true
          case _: DimIdInExpr                                            => true
          case _: DimIdNotInExpr                                         => true
          case BinaryOperationExpr(op, _: DimensionExpr, ConstantExpr(_)) if Set("==", "!=").contains(op.name) =>
            true
          case BinaryOperationExpr(op, ConstantExpr(_), _: DimensionExpr) if Set("==", "!=").contains(op.name) =>
            true
          case InExpr(_: DimensionExpr, _)    => true
          case NotInExpr(_: DimensionExpr, _) => true
          case _                              => false
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
      v.get[String](exprIndex, DimensionExpr(catalog.dimension)).foreach { tagValue =>
        catalogValues.row(tagValue).foreach {
          case (field, value) =>
            v.set(exprIndex, LinkExpr(catalog, field), Some(value))
        }
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
      .fold(fail(_), identity)
  }
}
