package org.yupana.jdbc

import java.sql.{ Connection, ResultSet }

import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType
import org.yupana.jdbc.build.BuildInfo
import org.yupana.proto.Version

class YupanaDatabaseMetaDataTest extends FlatSpec with Matchers with MockFactory {

  "YupanaDatabaseMetaData" should "provide common Yupana capabilities info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    m.getMaxColumnsInSelect shouldEqual 0
    m.getMaxColumnsInGroupBy shouldEqual 0
    m.getMaxColumnsInOrderBy shouldEqual 0

    m.getMaxColumnsInTable shouldEqual 0
    m.getMaxColumnNameLength shouldEqual 0
    m.getMaxTableNameLength shouldEqual 0
    m.getMaxCursorNameLength shouldEqual 0
    m.getMaxSchemaNameLength shouldEqual 0

    m.getMaxCharLiteralLength shouldEqual 0
    m.getMaxBinaryLiteralLength shouldEqual 0
    m.getMaxIndexLength shouldEqual 0
    m.getMaxRowSize shouldEqual 0

    m.getSchemaTerm shouldEqual "schema"

    m.allProceduresAreCallable shouldBe false
    m.allTablesAreSelectable shouldBe true

    m.supportsMinimumSQLGrammar shouldBe false
    m.supportsCoreSQLGrammar shouldBe false
    m.supportsExtendedSQLGrammar shouldBe false
    m.supportsANSI92EntryLevelSQL shouldBe false
    m.supportsANSI92IntermediateSQL shouldBe false
    m.supportsANSI92FullSQL shouldBe false
    m.supportsNamedParameters shouldBe false
    m.supportsTableCorrelationNames shouldBe false
    m.supportsDifferentTableCorrelationNames shouldBe false

    m.getMaxColumnsInOrderBy shouldEqual 0
    m.supportsExpressionsInOrderBy shouldBe false
    m.supportsOrderByUnrelated shouldBe false
    m.nullsAreSortedAtEnd shouldBe false
    m.nullsAreSortedAtStart shouldBe false
    m.nullsAreSortedLow shouldBe false
    m.nullsAreSortedHigh shouldBe false

    m.nullPlusNonNullIsNull shouldBe true

    m.supportsAlterTableWithAddColumn shouldBe false
    m.supportsAlterTableWithDropColumn shouldBe false

    m.getCatalogTerm shouldEqual "catalog"
    m.getCatalogs.next shouldBe false
    m.supportsCatalogsInDataManipulation shouldBe false
    m.supportsCatalogsInIndexDefinitions shouldBe false
    m.supportsCatalogsInPrivilegeDefinitions shouldBe false
    m.supportsCatalogsInProcedureCalls shouldBe false
    m.supportsCatalogsInTableDefinitions shouldBe false
    m.isCatalogAtStart shouldBe false

    m.supportsBatchUpdates shouldBe false
    m.supportsSelectForUpdate shouldBe false
    m.supportsPositionedUpdate shouldBe false

    m.supportsPositionedDelete shouldBe false

    Seq(ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.TYPE_SCROLL_INSENSITIVE).foreach { t =>
      m.insertsAreDetected(t) shouldBe false
      m.updatesAreDetected(t) shouldBe false
      m.deletesAreDetected(t) shouldBe false

      m.othersInsertsAreVisible(t) shouldBe false
      m.othersUpdatesAreVisible(t) shouldBe false
      m.othersDeletesAreVisible(t) shouldBe false

      m.ownInsertsAreVisible(t) shouldBe false
      m.ownUpdatesAreVisible(t) shouldBe false
      m.ownDeletesAreVisible(t) shouldBe false
    }

    m.supportsColumnAliasing shouldBe true

    m.supportsTransactions shouldBe false
    m.supportsMultipleTransactions shouldBe false
    m.dataDefinitionCausesTransactionCommit shouldBe false
    m.dataDefinitionIgnoredInTransactions shouldBe false
    m.supportsDataDefinitionAndDataManipulationTransactions shouldBe false
    m.supportsDataManipulationTransactionsOnly shouldBe false
    m.getDefaultTransactionIsolation shouldEqual Connection.TRANSACTION_NONE

    m.supportsStoredProcedures shouldBe false
    m.getProcedureTerm shouldEqual "procedure"
    m.allProceduresAreCallable shouldBe false
    m.getMaxProcedureNameLength shouldBe 0
    m.getProcedures("", "", "").next() shouldBe false

    val tts = m.getTableTypes
    val tt = Iterator.continually(tts).takeWhile(_.next()).map(r => r.getString("TABLE_TYPE")).toSeq
    tt shouldEqual Seq("TABLE")
  }

  it should "provide driver info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    m.getDriverName shouldEqual "Yupana JDBC"
    m.getDriverMajorVersion shouldEqual BuildInfo.majorVersion
    m.getDriverMinorVersion shouldEqual BuildInfo.minorVersion
    m.getDriverVersion shouldEqual BuildInfo.version

    m.getJDBCMajorVersion shouldEqual 4
    m.getJDBCMinorVersion shouldEqual 1
  }

  it should "provide connection info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    (conn.url _).expects().returning("jdbc:yupana://example.com:10101")
    m.getURL shouldEqual "jdbc:yupana://example.com:10101"

    (conn.serverVersion _).expects().returning(Some(Version(9, 1, 2, "1.2.3"))).anyNumberOfTimes()
    m.getDatabaseMajorVersion shouldEqual 1
    m.getDatabaseMinorVersion shouldEqual 2
    m.getDatabaseProductVersion shouldEqual "1.2.3"
    m.getDatabaseProductName shouldEqual "Yupana"

    m.getUserName shouldEqual ""
  }

  it should "provide table info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    val tables = SimpleResult(
      "TABLES",
      Seq("TABLE_NAME", "TABLE_TYPE"),
      Seq(DataType[String], DataType[String]),
      Seq(
        Array[Option[Any]](Some("EMPLOYEES"), None),
        Array[Option[Any]](Some("DEPARTMENTS"), Some("TABLE"))
      ).iterator
    )

    (conn.createStatement _).expects().returning(new YupanaStatement(conn))
    (conn.runQuery _).expects("SHOW TABLES", Map.empty[Int, ParameterValue]).returning(tables)

    val rs = m.getTables("", "", "", Array.empty)
    val result = Iterator.continually(rs).takeWhile(_.next()).map(_.getString("TABLE_NAME")).toSeq
    result should contain theSameElementsAs Seq("EMPLOYEES", "DEPARTMENTS")
  }
}
