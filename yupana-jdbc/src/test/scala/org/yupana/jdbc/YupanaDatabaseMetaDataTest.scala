package org.yupana.jdbc

import java.sql.{ Connection, DatabaseMetaData, ResultSet, RowIdLifetime, Types }

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

    m.isReadOnly shouldBe true
    m.getMaxTablesInSelect shouldEqual 1

    m.getMaxConnections shouldEqual 0
    m.getMaxColumnsInIndex shouldEqual 0
    m.getMaxColumnsInSelect shouldEqual 0
    m.getMaxColumnsInGroupBy shouldEqual 0
    m.getMaxColumnsInOrderBy shouldEqual 0
    m.getMaxUserNameLength shouldEqual 0

    m.getMaxColumnsInTable shouldEqual 0
    m.getMaxColumnNameLength shouldEqual 0
    m.getMaxCatalogNameLength shouldEqual 0
    m.getMaxTableNameLength shouldEqual 0
    m.getMaxCursorNameLength shouldEqual 0
    m.getMaxSchemaNameLength shouldEqual 0
    m.getMaxStatementLength shouldEqual 0

    m.getMaxCharLiteralLength shouldEqual 0
    m.getMaxBinaryLiteralLength shouldEqual 0
    m.getMaxIndexLength shouldEqual 0
    m.getMaxRowSize shouldEqual 0
    m.doesMaxRowSizeIncludeBlobs shouldBe false

    m.getMaxStatements shouldEqual 0
    m.supportsMultipleOpenResults shouldBe true
    m.supportsMultipleResultSets shouldBe false

    m.getSchemaTerm shouldEqual "schema"
    m.getSchemas.next shouldBe false
    m.supportsSchemasInDataManipulation shouldBe false
    m.supportsSchemasInIndexDefinitions shouldBe false
    m.supportsSchemasInPrivilegeDefinitions shouldBe false
    m.supportsSchemasInProcedureCalls shouldBe false
    m.supportsSchemasInTableDefinitions shouldBe false

    m.allProceduresAreCallable shouldBe false
    m.allTablesAreSelectable shouldBe true

    m.getSQLStateType shouldEqual DatabaseMetaData.sqlStateSQL

    m.supportsMinimumSQLGrammar shouldBe false
    m.supportsCoreSQLGrammar shouldBe false
    m.supportsExtendedSQLGrammar shouldBe false
    m.supportsANSI92EntryLevelSQL shouldBe false
    m.supportsANSI92IntermediateSQL shouldBe false
    m.supportsANSI92FullSQL shouldBe false
    m.supportsNamedParameters shouldBe false
    m.supportsTableCorrelationNames shouldBe false
    m.supportsDifferentTableCorrelationNames shouldBe false
    m.supportsNonNullableColumns shouldBe false

    m.supportsGroupBy shouldBe true
    m.supportsGroupByUnrelated() shouldBe true
    m.supportsGroupByBeyondSelect() shouldBe true

    m.supportsOuterJoins shouldBe false
    m.supportsLimitedOuterJoins shouldBe false
    m.supportsFullOuterJoins shouldBe false

    m.supportsUnion shouldBe false
    m.supportsUnionAll shouldBe false

    m.supportsLikeEscapeClause shouldBe false

    m.supportsSubqueriesInComparisons shouldBe false
    m.supportsSubqueriesInExists shouldBe false
    m.supportsSubqueriesInIns shouldBe false
    m.supportsSubqueriesInQuantifieds shouldBe false
    m.supportsCorrelatedSubqueries shouldBe false

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
    m.autoCommitFailureClosesAllResultSets shouldBe false
    m.dataDefinitionIgnoredInTransactions shouldBe false
    m.supportsDataDefinitionAndDataManipulationTransactions shouldBe false
    m.supportsDataManipulationTransactionsOnly shouldBe false
    m.getDefaultTransactionIsolation shouldEqual Connection.TRANSACTION_NONE

    m.supportsSavepoints shouldBe false

    m.supportsStoredProcedures shouldBe false
    m.supportsStoredFunctionsUsingCallSyntax() shouldBe false
    m.getProcedureTerm shouldEqual "procedure"
    m.allProceduresAreCallable shouldBe false
    m.getMaxProcedureNameLength shouldBe 0
    m.getProcedures("", "", "").next() shouldBe false

    m.generatedKeyAlwaysReturned shouldBe false
    m.supportsGetGeneratedKeys shouldBe false

    m.storesMixedCaseIdentifiers shouldBe false
    m.storesMixedCaseQuotedIdentifiers shouldBe false
    m.supportsMixedCaseIdentifiers shouldBe false
    m.supportsMixedCaseQuotedIdentifiers shouldBe false
    m.storesLowerCaseIdentifiers shouldBe true
    m.storesLowerCaseQuotedIdentifiers shouldBe true
    m.storesUpperCaseIdentifiers shouldBe false
    m.storesUpperCaseQuotedIdentifiers shouldBe false

    m.supportsOpenCursorsAcrossCommit shouldBe false
    m.supportsOpenCursorsAcrossRollback shouldBe false
    m.supportsOpenCursorsAcrossRollback shouldBe false
    m.supportsRefCursors shouldBe false

    m.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT) shouldBe false
    m.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT) shouldBe true
    m.getResultSetHoldability shouldEqual ResultSet.CLOSE_CURSORS_AT_COMMIT
    m.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) shouldBe true
    m.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY) shouldBe false
    m.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY) shouldBe true
    m.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE) shouldBe false

    m.getImportedKeys("", "", "kkmItems").next() shouldBe false

    m.supportsOpenCursorsAcrossRollback() shouldBe false
    m.supportsIntegrityEnhancementFacility() shouldBe false
    m.supportsStatementPooling() shouldBe false

    m.supportsConvert() shouldBe false
    m.supportsConvert(Types.INTEGER, Types.DECIMAL) shouldBe false

    m.locatorsUpdateCopy shouldBe false
    m.usesLocalFiles shouldBe false
    m.usesLocalFilePerTable shouldBe false
    m.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE) shouldBe true
    m.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED) shouldBe false

    m.getRowIdLifetime shouldEqual RowIdLifetime.ROWID_UNSUPPORTED

    m.getSearchStringEscape shouldEqual "\\"
    m.getIdentifierQuoteString shouldEqual "\""

    val tts = m.getTableTypes
    val tt = Iterator.continually(tts).takeWhile(_.next()).map(r => r.getString("TABLE_TYPE")).toSeq
    tt shouldEqual Seq("TABLE")
  }

  it should "provide driver info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    m.getConnection shouldEqual conn

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

    (conn.getUrl _).expects().returning("jdbc:yupana://example.com:10101")
    m.getURL shouldEqual "jdbc:yupana://example.com:10101"

    (conn.getServerVersion _).expects().returning(Some(Version(9, 1, 2, "1.2.3"))).anyNumberOfTimes()
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
        Array[Any]("EMPLOYEES", null),
        Array[Any]("DEPARTMENTS", "TABLE")
      ).iterator
    )

    (conn.createStatement _).expects().returning(new YupanaStatement(conn))
    (conn.runQuery _).expects("SHOW TABLES", Map.empty[Int, ParameterValue]).returning(tables)

    val rs = m.getTables("", "", "", Array.empty)
    val result = Iterator.continually(rs).takeWhile(_.next()).map(_.getString("TABLE_NAME")).toSeq
    result should contain theSameElementsAs Seq("EMPLOYEES", "DEPARTMENTS")
  }

  it should "provide column info" in {
    val conn = mock[YupanaConnection]
    val m = new YupanaDatabaseMetaData(conn)

    val tables = SimpleResult(
      "COLUMNS",
      Seq("TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"),
      Seq(DataType[String], DataType[String]),
      Seq(
        Array[Any]("EMPLOYEES", "NAME", "VARCHAR"),
        Array[Any]("EMPLOYEES", "AGE", "INTEGER")
      ).iterator
    )

    (conn.createStatement _).expects().returning(new YupanaStatement(conn))
    (conn.runQuery _).expects("SHOW COLUMNS FROM EMPLOYEES", Map.empty[Int, ParameterValue]).returning(tables)

    val rs = m.getColumns("", "", "EMPLOYEES", "")
    val result = Iterator.continually(rs).takeWhile(_.next()).map(_.getString("COLUMN_NAME")).toSeq
    result should contain theSameElementsAs Seq("NAME", "AGE")
  }
}
