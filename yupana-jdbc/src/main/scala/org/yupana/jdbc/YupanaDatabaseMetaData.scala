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

package org.yupana.jdbc

import java.sql.{ Connection, DatabaseMetaData, ResultSet, RowIdLifetime, SQLException }

import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.jdbc.build.BuildInfo

class YupanaDatabaseMetaData(connection: YupanaConnection) extends DatabaseMetaData {

  private def emptyResultSet = new YupanaResultSet(null, Result.empty)

  override def supportsMinimumSQLGrammar(): Boolean = false

  override def getResultSetHoldability: Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  override def getMaxColumnsInGroupBy: Int = 0

  override def supportsSubqueriesInComparisons(): Boolean = false

  override def getMaxColumnsInSelect: Int = 0

  override def nullPlusNonNullIsNull() = true

  override def supportsCatalogsInDataManipulation() = false

  override def supportsDataDefinitionAndDataManipulationTransactions() = false

  override def supportsTableCorrelationNames() = false

  override def getDefaultTransactionIsolation: Int = Connection.TRANSACTION_NONE

  override def supportsFullOuterJoins(): Boolean = false

  override def supportsExpressionsInOrderBy() = false

  override def allProceduresAreCallable() = false

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean =
    `type` == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY

  override def getMaxTablesInSelect = 1

  override def nullsAreSortedAtStart() = false

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = emptyResultSet

  override def supportsPositionedUpdate() = false

  override def ownDeletesAreVisible(`type`: Int) = false

  override def supportsResultSetHoldability(holdability: Int): Boolean =
    holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT

  override def getMaxStatements = 0

  override def getRowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED

  override def getSchemaTerm = "schema"

  override def getMaxCatalogNameLength = 0

  override def getCrossReference(
      parentCatalog: String,
      parentSchema: String,
      parentTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String
  ): ResultSet = emptyResultSet

  override def getCatalogTerm = "catalog"

  override def getAttributes(
      catalog: String,
      schemaPattern: String,
      typeNamePattern: String,
      attributeNamePattern: String
  ): ResultSet = emptyResultSet

  override def getMaxStatementLength = 0

  override def supportsOuterJoins() = false

  override def supportsBatchUpdates() = false

  override def supportsLimitedOuterJoins() = false

  override def getMaxColumnsInTable = 0

  override def allTablesAreSelectable() = true

  override def getMaxCharLiteralLength = 0

  override def supportsMultipleOpenResults() = true

  override def getMaxRowSize = 0

  override def supportsUnion() = false

  override def supportsOpenCursorsAcrossCommit() = false

  override def ownUpdatesAreVisible(`type`: Int) = false

  override def getSearchStringEscape = "\\"

  override def getMaxBinaryLiteralLength = 0

  override def supportsAlterTableWithDropColumn = false

  override def supportsResultSetType(`type`: Int): Boolean = `type` == ResultSet.TYPE_FORWARD_ONLY

  override def supportsCatalogsInProcedureCalls() = false

  override def supportsSelectForUpdate() = false

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet =
    emptyResultSet

  override def supportsOpenStatementsAcrossRollback() = false

  override def getSystemFunctions = ""

  override def supportsColumnAliasing() = true

  override def insertsAreDetected(`type`: Int) = false

  override def supportsMixedCaseIdentifiers() = false

  override def getDatabaseProductVersion: String = connection.serverVersion.map(_.version).getOrElse("Unknown")

  override def getDatabaseMajorVersion: Int = connection.serverVersion.map(_.major).getOrElse(0)

  override def getDatabaseMinorVersion: Int = connection.serverVersion.map(_.minor).getOrElse(0)

  override def getSQLKeywords = ""

  override def dataDefinitionIgnoredInTransactions = false

  override def getJDBCMajorVersion = 4

  override def getJDBCMinorVersion = 1

  override def getMaxColumnNameLength = 0

  override def isReadOnly = true

  override def getProcedureColumns(
      catalog: String,
      schemaPattern: String,
      procedureNamePattern: String,
      columnNamePattern: String
  ): ResultSet = emptyResultSet

  override def getCatalogs: ResultSet = emptyResultSet

  override def locatorsUpdateCopy = false

  override def supportsANSI92FullSQL() = false

  override def supportsMultipleResultSets() = false

  override def storesUpperCaseIdentifiers() = false

  override def supportsSchemasInPrivilegeDefinitions() = false

  override def getDriverName = "Yupana JDBC"

  override def getDriverVersion: String = BuildInfo.version

  override def getDriverMajorVersion: Int = BuildInfo.majorVersion

  override def getDriverMinorVersion: Int = BuildInfo.minorVersion

  override def getMaxConnections = 0

  override def othersUpdatesAreVisible(`type`: Int) = false

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = emptyResultSet

  override def supportsNamedParameters() = false

  override def doesMaxRowSizeIncludeBlobs = false

  override def getBestRowIdentifier(
      catalog: String,
      schema: String,
      table: String,
      scope: Int,
      nullable: Boolean
  ): ResultSet = emptyResultSet

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet =
    emptyResultSet

  override def supportsSubqueriesInQuantifieds() = false

  override def getMaxSchemaNameLength = 0

  override def supportsIntegrityEnhancementFacility() = false

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet =
    emptyResultSet

  override def supportsExtendedSQLGrammar() = false

  override def supportsConvert() = false

  override def supportsConvert(fromType: Int, toType: Int) = false

  override def getFunctionColumns(
      catalog: String,
      schemaPattern: String,
      functionNamePattern: String,
      columnNamePattern: String
  ): ResultSet = emptyResultSet

  override def supportsPositionedDelete() = false

  override def autoCommitFailureClosesAllResultSets() = false

  override def getMaxColumnsInOrderBy = 0

  override def supportsANSI92IntermediateSQL() = false

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet =
    emptyResultSet

  override def supportsMultipleTransactions() = false

  override def supportsCatalogsInTableDefinitions() = false

  override def supportsOpenCursorsAcrossRollback() = false

  override def supportsStatementPooling() = false

  override def usesLocalFiles() = false

  override def storesMixedCaseQuotedIdentifiers() = false

  override def othersInsertsAreVisible(`type`: Int) = false

  override def supportsSchemasInProcedureCalls() = false

  override def getMaxCursorNameLength = 0

  override def getUserName = ""

  override def supportsTransactionIsolationLevel(level: Int): Boolean = level == Connection.TRANSACTION_NONE

  override def deletesAreDetected(`type`: Int) = false

  override def supportsDataManipulationTransactionsOnly() = false

  override def supportsLikeEscapeClause() = false

  override def supportsSchemasInDataManipulation() = false

  override def supportsGetGeneratedKeys() = false

  override def supportsGroupBy() = true

  override def getIndexInfo(
      catalog: String,
      schema: String,
      table: String,
      unique: Boolean,
      approximate: Boolean
  ): ResultSet = emptyResultSet

  override def getSchemas: ResultSet = emptyResultSet

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = emptyResultSet

  override def supportsSchemasInIndexDefinitions() = false

  override def getCatalogSeparator = ""

  override def getExtraNameCharacters = ""

  override def getURL: String = connection.url

  override def supportsDifferentTableCorrelationNames() = false

  override def supportsTransactions() = false

  override def storesLowerCaseQuotedIdentifiers() = true

  override def supportsANSI92EntryLevelSQL() = false

  override def supportsStoredProcedures() = false

  override def supportsCatalogsInPrivilegeDefinitions() = false

  override def getIdentifierQuoteString = "\""

  override def getMaxTableNameLength = 0

  override def getSQLStateType: Int = DatabaseMetaData.sqlStateSQL

  override def getColumnPrivileges(
      catalog: String,
      schema: String,
      table: String,
      columnNamePattern: String
  ): ResultSet = emptyResultSet

  override def getMaxColumnsInIndex = 0

  override def getTableTypes: ResultSet = {
    val names = List("TABLE_TYPE")
    val dataTypes = List(DataType[String])
    val types = SimpleResult("TYPES", names, dataTypes, Iterator(Array[Any]("TABLE"), Array[Any]("ROLLUP")))
    new YupanaResultSet(null, types)
  }

  override def nullsAreSortedHigh() = false

  override def supportsNonNullableColumns() = false

  override def getMaxUserNameLength = 0

  override def supportsSubqueriesInExists() = false

  override def ownInsertsAreVisible(`type`: Int): Boolean = false

  override def supportsOpenStatementsAcrossCommit() = false

  override def supportsSavepoints() = false

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet =
    emptyResultSet

  override def dataDefinitionCausesTransactionCommit = false

  override def nullsAreSortedAtEnd() = false

  override def getTimeDateFunctions: String = getFunctionsForType("TIMESTAMP")

  override def getNumericFunctions: String = getFunctionsForType("DECIMAL")

  override def getStringFunctions: String = getFunctionsForType("VARCHAR")

  override def generatedKeyAlwaysReturned = false

  override def supportsUnionAll() = false

  override def supportsAlterTableWithAddColumn() = false

  override def isCatalogAtStart = false

  override def othersDeletesAreVisible(`type`: Int) = false

  override def supportsCoreSQLGrammar() = false

  override def getMaxProcedureNameLength = 0

  override def getColumns(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String
  ): ResultSet = {
    val sql = s"SHOW COLUMNS FROM $tableNamePattern"
    val stmt = connection.createStatement()
    stmt.executeQuery(sql)
  }

  override def getConnection: Connection = connection

  override def getDatabaseProductName = "Yupana"

  override def supportsGroupByUnrelated() = true

  override def nullsAreSortedLow() = false

  override def getTables(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      types: Array[String]
  ): ResultSet = {
    val sql = "SHOW TABLES"
    val stmt = connection.createStatement()
    stmt.executeQuery(sql)
  }

  override def supportsCorrelatedSubqueries() = false

  override def supportsMixedCaseQuotedIdentifiers() = false

  override def supportsGroupByBeyondSelect() = true

  override def supportsCatalogsInIndexDefinitions() = false

  override def supportsOrderByUnrelated() = false

  override def getMaxIndexLength = 0

  override def getProcedureTerm = "procedure"

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet =
    emptyResultSet

  override def getClientInfoProperties: ResultSet = emptyResultSet

  override def supportsStoredFunctionsUsingCallSyntax() = false

  override def getPseudoColumns(
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String
  ): ResultSet = emptyResultSet

  override def usesLocalFilePerTable() = false

  override def storesLowerCaseIdentifiers() = true

  override def supportsSubqueriesInIns() = false

  override def updatesAreDetected(`type`: Int) = false

  // TODO: Implement me
  override def getTypeInfo: ResultSet = emptyResultSet

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = emptyResultSet

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = emptyResultSet

  override def supportsSchemasInTableDefinitions() = false

  override def storesUpperCaseQuotedIdentifiers() = false

  override def storesMixedCaseIdentifiers() = false

  override def unwrap[T](iface: Class[T]): T = {
    if (!iface.isAssignableFrom(getClass)) {
      throw new SQLException(s"Cannot unwrap to ${iface.getName}")
    }

    iface.cast(this)
  }

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isAssignableFrom(getClass)

  private def getFunctionsForType(t: String): String = {
    val sql = s"SHOW FUNCTIONS FOR $t"
    val stmt = connection.createStatement()
    val rs = stmt.executeQuery(sql)
    val fs = Iterator.continually(rs).takeWhile(_.next()).map(_.getString("NAME")).toSeq
    fs mkString ","
  }
}
