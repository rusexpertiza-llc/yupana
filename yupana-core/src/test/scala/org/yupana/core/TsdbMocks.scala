package org.yupana.core

import org.scalamock.scalatest.MockFactory
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.core.dao.{DictionaryDao, DictionaryProviderImpl}
import org.yupana.core.model.InternalRow
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{Select, SqlParser}
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

    (catalogService.isSupportedCondition _).stubs(*).onCall((condition: Condition) =>
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, LinkExpr(c, _), ConstantExpr(_))) if Set("==", "!=").contains(op.name) && c.linkName == catalog.linkName => true
        case In(LinkExpr(c, _), _) if c.linkName == catalog.linkName => true
        case NotIn(LinkExpr(c, _), _) if c.linkName == catalog.linkName => true
        case _ => false
      }
    )

    catalogService
  }

  def withTsdbMock(body: (TSDB, TSTestDao) => Unit): Unit = {
    val tsdbDaoMock = mock[TSTestDao]
    (tsdbDaoMock.isSupportedCondition _).expects(*).onCall((c: Condition) =>
      c match  {
        case SimpleCondition(BinaryOperationExpr(_, _: TimeExpr.type, ConstantExpr(_))) => true
        case SimpleCondition(BinaryOperationExpr(_, ConstantExpr(_), _: TimeExpr.type)) => true
        case _: DimIdIn => true
        case _: DimIdNotIn => true
        case SimpleCondition(BinaryOperationExpr(op, _: DimensionExpr, ConstantExpr(_))) if Set("==", "!=").contains(op.name) => true
        case SimpleCondition(BinaryOperationExpr(op, ConstantExpr(_), _: DimensionExpr)) if Set("==", "!=").contains(op.name) => true
        case In(_: DimensionExpr, _) => true
        case NotIn(_: DimensionExpr, _) => true
        case _ => false
      }
    ).anyNumberOfTimes()
    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val tsdb = new TSDB(tsdbDaoMock, dictionaryProvider, identity)
    body(tsdb, tsdbDaoMock)
  }

  def setCatalogValueByTag(exprIndex: scala.collection.Map[Expression, Int], datas: Seq[InternalRow], catalog: ExternalLink, catalogValues: Table[String, String, String]): Unit = {
    datas.foreach { v =>
      v.get[String](exprIndex, DimensionExpr(catalog.dimension)).foreach { tagValue =>
        catalogValues.row(tagValue).foreach { case (field, value) =>
          v.set(exprIndex, LinkExpr(catalog, field), Some(value))
        }
      }
    }
  }

  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)

  def createQuery(sql: String): Query = {
    SqlParser.parse(sql).right.flatMap {
      case s: Select => sqlQueryProcessor.createQuery(s)
      case x => Left(s"SELECT statement expected, but got $x")
    }.fold(fail(_), identity)
  }
}
