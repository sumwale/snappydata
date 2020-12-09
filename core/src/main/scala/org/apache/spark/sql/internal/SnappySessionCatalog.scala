/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.internal

import java.io.File
import java.net.URL
import java.sql.SQLException

import scala.util.control.NonFatal

import com.gemstone.gemfire.SystemFailure
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.Constant
import io.snappydata.sql.catalog.CatalogObjectType.getTableType
import io.snappydata.sql.catalog.SnappyExternalCatalog.{DBTABLE_PROPERTY, getTableWithDatabase}
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchFunctionException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, IdentifierWithDatabase, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.TopK
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.hive.{HiveSessionCatalog, SnappyHiveExternalCatalog}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.{DestroyRelation, JdbcExtendedUtils, MutableRelation, RowLevelSecurityRelation}
import org.apache.spark.sql.types._
import org.apache.spark.util.MutableURLClassLoader

/**
 * SessionCatalog implementation using Snappy store for persistence in embedded mode and
 * using client API calls for smart connector mode, Adds Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 */
trait SnappySessionCatalog extends SessionCatalog with SparkSupport {

  def snappyExternalCatalog: SnappyExternalCatalog
  def globalTempManager: GlobalTempViewManager
  val functionResourceLoader: FunctionResourceLoader
  val functionRegistry: FunctionRegistry
  val snappySession: SnappySession
  val sqlConf: SQLConf
  val parser: SnappySqlParser
  val wrappedCatalog: Option[SnappySessionCatalog]

  def functionNotFound(name: String): Unit

  final def contextFunctions: SnappyContextFunctions = snappySession.contextFunctions

  /**
   * Can be used to temporarily switch the metadata returned by catalog
   * to use CharType and VarcharTypes. Is to be used for only temporary
   * change by a caller that wishes the consume the result because rest
   * of Spark cannot deal with those types.
   */
  protected[sql] var convertCharTypesInMetadata = false

  private[this] var skipDefaultDatabases = false

  // initialize default database/schema
  val defaultDbName: String = {
    var defaultName = snappySession.conf.get(Attribute.USERNAME_ATTR, "")
    if (defaultName.isEmpty) {
      // In smart connector, property name is different.
      defaultName = snappySession.conf.get(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR,
        Constant.DEFAULT_DATABASE)
    }
    defaultName = formatDatabaseName(IdUtil.getUserAuthorizationId(defaultName).replace('-', '_'))
    createDatabase(defaultName, ignoreIfExists = true)
    setCurrentDatabase(defaultName, force = true)
    defaultName
  }

  /**
   * Format table name. Hive meta-store is case-insensitive so always convert to lower case.
   */
  override def formatTableName(name: String): String = formatName(name)

  /**
   * Format database name. Hive meta-store is case-insensitive so always convert to lower case.
   */
  override def formatDatabaseName(name: String): String = formatName(name)

  /**
   * Fallback session state to lookup from external hive catalog in case
   * "snappydata.sql.hive.enabled" is set on the session.
   */
  protected[sql] final lazy val hiveSessionCatalog: HiveSessionCatalog =
    snappySession.snappySessionState.hiveState.catalog.asInstanceOf[HiveSessionCatalog]

  /**
   * Return true if the given table needs to be checked in the builtin catalog
   * rather than the external hive catalog (if enabled).
   */
  protected final def checkBuiltinCatalog(tableIdent: TableIdentifier): Boolean =
    !snappySession.enableHiveSupport || super.tableExists(tableIdent)

  final def formatName(name: String): String =
    if (snappySession.snappyParser.caseSensitive) name else JdbcExtendedUtils.toLowerCase(name)

  /** API to get primary key or Key Columns of a SnappyData table */
  def getKeyColumns(table: String): Seq[Column] = getKeyColumnsAndPositions(table).map(_._1)

  /** API to get primary key or Key Columns of a SnappyData table */
  def getKeyColumnsAndPositions(table: String): Seq[(Column, Int)] = {
    val tableIdent = snappySession.tableIdentifier(table)
    val relation = resolveRelation(tableIdent)
    val keyColumns = relation match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[MutableRelation] =>
        val keyCols = lr.relation.asInstanceOf[MutableRelation].getPrimaryKeyColumns(snappySession)
        if (keyCols.isEmpty) {
          Nil
        } else {
          val tableMetadata = this.getTempViewOrPermanentTableMetadata(tableIdent)
          val tableSchema = tableMetadata.schema.zipWithIndex
          val fieldsInMetadata =
            keyCols.map(k => tableSchema.find(p => p._1.name.equalsIgnoreCase(k)) match {
              case None => throw new AnalysisException(s"Invalid key column name $k")
              case Some(p) => p
            })
          fieldsInMetadata.map { p =>
            val c = p._1
            new Column(
              name = c.name,
              description = c.getComment().orNull,
              dataType = c.dataType.catalogString,
              nullable = c.nullable,
              isPartition = false, // Setting it to false for SD tables
              isBucket = false) -> p._2
          }
        }
      case _ => Nil
    }
    keyColumns
  }

  def compatibleSchema(schema1: StructType, schema2: StructType): Boolean = {
    schema1.fields.length == schema2.fields.length &&
        !schema1.zip(schema2).exists { case (f1, f2) =>
          !f1.dataType.sameType(f2.dataType)
        }
  }

  /*
  final def getCombinedPolicyFilterForExternalTable(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation],
      currentUser: Option[String]): Option[Filter] = {
    // filter out policy rows
    // getCombinedPolicyFilter(rlsRelation, wrappingLogicalRelation, currentUser)
    None
  }
  */

  final def getCombinedPolicyFilterForNativeTable(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation]): Option[Filter] = {
    // filter out policy rows
    getCombinedPolicyFilter(rlsRelation, wrappingLogicalRelation)
  }

  private def getCombinedPolicyFilter(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation]): Option[Filter] = {
    if (!rlsRelation.isRowLevelSecurityEnabled) {
      None
    } else {
      val catalogTable = getTableMetadata(new TableIdentifier(
        rlsRelation.tableName, Some(rlsRelation.schemaName)))
      val policyFilters = snappyExternalCatalog.getPolicies(rlsRelation.schemaName,
        rlsRelation.tableName, catalogTable.properties).map { ct =>
        resolveRelation(ct.identifier).asInstanceOf[BypassRowLevelSecurity].child
      }
      if (policyFilters.isEmpty) None
      else {
        val combinedPolicyFilters = policyFilters.foldLeft[Filter](null) {
          case (result, filter) =>
            if (result == null) {
              filter
            } else {
              result.copy(condition = org.apache.spark.sql.catalyst.expressions.And(
                filter.condition, result.condition))
            }
        }
        val storedLogicalRelation = resolveRelation(snappySession.tableIdentifier(
          rlsRelation.resolvedName)).find {
          case _: LogicalRelation => true
          case _ => false
        }.get.asInstanceOf[LogicalRelation]

        Some(remapFilterIfNeeded(combinedPolicyFilters, wrappingLogicalRelation,
          storedLogicalRelation))
      }
    }
  }

  private def remapFilterIfNeeded(filter: Filter, queryLR: Option[LogicalRelation],
      storedLR: LogicalRelation): Filter = {
    if (queryLR.isEmpty || queryLR.get.output.
        corresponds(storedLR.output)((a1, a2) => a1.exprId == a2.exprId)) {
      filter
    } else {
      // remap filter
      val mappingInfo = storedLR.output.map(_.exprId).zip(
        queryLR.get.output.map(_.exprId)).toMap
      internals.logicalPlanResolveExpressions(filter) {
        case ar: AttributeReference if mappingInfo.contains(ar.exprId) =>
          internals.toAttributeReference(ar)(exprId = mappingInfo(ar.exprId))
      }.asInstanceOf[Filter]
    }
  }

  final def getDbName(identifier: IdentifierWithDatabase): String = identifier.database match {
    case None => getCurrentDatabase
    case Some(s) => formatDatabaseName(s)
  }

  /** Add database to TableIdentifier if missing and format the name. */
  final def resolveTableIdentifier(identifier: TableIdentifier): TableIdentifier = {
    TableIdentifier(formatTableName(identifier.table), Some(getDbName(identifier)))
  }

  private def qualifiedTableIdentifier(identifier: TableIdentifier,
      dbName: String): TableIdentifier = {
    if (identifier.database.isDefined) identifier
    else identifier.copy(database = Some(dbName))
  }

  private def resolveCatalogTable(table: CatalogTable, dbName: String): CatalogTable = {
    if (table.identifier.database.isDefined) table
    else table.copy(identifier = table.identifier.copy(database = Some(dbName)))
  }

  /** Convert a table name to TableIdentifier for an existing table. */
  final def resolveExistingTable(name: String): TableIdentifier = {
    val identifier = snappySession.tableIdentifier(name)
    if (isTemporaryTable(identifier)) identifier
    else TableIdentifier(identifier.table, Some(getDbName(identifier)))
  }

  /**
   * Lookup relation and resolve to a LogicalRelation if not a temporary view.
   */
  final def resolveRelationWithAlias(tableIdent: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    // resolve the relation right away with alias around
    new FindDataSourceTable(snappySession)(lookupRelationImpl(tableIdent, alias, wrapped = None))
  }

  /**
   * Lookup relation and resolve to a LogicalRelation if not a temporary view.
   */
  final def resolveRelation(tableIdent: TableIdentifier): LogicalPlan = {
    // resolve the relation right away
    resolveRelationWithAlias(tableIdent) match {
      case s: SubqueryAlias => s.child
      case p => p // external hive table
    }
  }

  // NOTE: Many of the overrides below are due to SnappyData allowing absence of
  // "global_temp" database to access global temporary views. Secondly checking for
  // database access permissions and creating database implicitly if required.

  /**
   * SnappyData allows the database for global temporary views to be optional so this method
   * adds it to TableIdentifier if required so that super methods can be invoked directly.
   */
  protected def addMissingGlobalTempDb(name: TableIdentifier): TableIdentifier = {
    if (name.database.isEmpty) {
      val tableName = formatTableName(name.table)
      if (globalTempManager.get(tableName).isDefined) {
        name.copy(table = tableName, database = Some(globalTempManager.database))
      } else name
    } else name
  }

  private[sql] def checkDbPermission(db: String, table: String,
      defaultUser: String, ignoreIfNotExists: Boolean = false): String = {
    SnappyExternalCatalog.checkDatabasePermission(db, table, defaultUser,
      snappySession.conf, ignoreIfNotExists)
  }

  protected[sql] def validateDbName(dbName: String, checkForDefault: Boolean): Unit = {
    if (dbName == globalTempManager.database) {
      throw new AnalysisException(s"$dbName is a system preserved database/schema for global " +
          s"temporary tables. You cannot create, drop or set a database with this name.")
    }
    if (checkForDefault && dbName == SnappyExternalCatalog.SPARK_DEFAULT_DATABASE) {
      throw new AnalysisException(s"$dbName is a system preserved database/schema.")
    }
  }

  def isLocalTemporaryView(name: TableIdentifier): Boolean = {
    name.database.isEmpty && getTempView(name.table).isDefined
  }

  private def dbDescription(dbName: String): String = s"User $dbName database"

  /**
   * Similar to createDatabase but uses pre-defined defaults for CatalogDatabase and never
   * creates in external hive catalog even if enabled. So this is for implicit database creations.
   * The passed dbName should already be formatted by a call to [[formatDatabaseName]].
   */
  private[sql] def createDatabase(dbName: String, ignoreIfExists: Boolean,
      createInStore: Boolean = true): Unit = {
    validateDbName(dbName, checkForDefault = false)

    // create database in catalog first
    if (snappyExternalCatalog.databaseExists(dbName)) {
      if (!ignoreIfExists) throw new AnalysisException(s"Database/Schema '$dbName' already exists")
    } else {
      super.createDatabase(CatalogDatabase(dbName, dbDescription(dbName),
        getDefaultDBPath(dbName), Map.empty), ignoreIfExists)
    }

    // then in store if catalog was successful
    if (createInStore) createStoreDatabase(dbName, ignoreIfExists)
  }

  private def createStoreDatabase(db: String, ignoreIfExists: Boolean,
      authClause: String = ""): Unit = {
    val conn = snappySession.defaultPooledConnection(db)
    try {
      val stmt = conn.createStatement()
      val storeDb = Utils.toUpperCase(db)
      // check for existing database
      if (ignoreIfExists) {
        val rs = stmt.executeQuery(s"select 1 from sys.sysschemas where schemaname='$storeDb'")
        if (rs.next()) {
          rs.close()
          stmt.close()
          return
        }
      }
      stmt.executeUpdate(s"CREATE SCHEMA `$storeDb`$authClause")
      stmt.close()
    } catch {
      case se: SQLException if ignoreIfExists && se.getSQLState == "X0Y68" => // ignore
      case err: Error if SystemFailure.isJVMFailureError(err) =>
        SystemFailure.initiateFailure(err)
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err
      case t: Throwable =>
        // drop from catalog
        dropDatabase(db, ignoreIfNotExists = true, cascade = false)
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure()
        throw t
    } finally {
      conn.close()
    }
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    validateDbName(dbName, checkForDefault = false)

    // create in catalogs first
    if (snappySession.enableHiveSupport) {
      hiveSessionCatalog.createDatabase(dbDefinition, ignoreIfExists)
    }
    super.createDatabase(dbDefinition, ignoreIfExists)

    // then in store if catalogs successfully created it
    createStoreDatabase(dbName, ignoreIfExists,
      Utils.getDatabaseAuthorizationClause(dbDefinition.properties))
  }

  /**
   * Drop all the objects in a database. The provided database must already be formatted
   * with a call to [[formatDatabaseName]].
   */
  def dropAllDatabaseObjects(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    val dbName = formatDatabaseName(db)
    if (dbName == SnappyExternalCatalog.SYS_DATABASE) {
      throw new AnalysisException(s"$dbName is a system preserved database/schema")
    }

    if (!snappyExternalCatalog.databaseExists(dbName)) {
      if (ignoreIfNotExists) return
      else throw SnappyExternalCatalog.databaseNotFoundException(dbName)
    }
    checkDbPermission(dbName, table = "", defaultUser = null, ignoreIfNotExists)

    if (cascade) {
      // drop all the tables in order first, dependents followed by others
      val allTableNames = snappyExternalCatalog.listTables(dbName)
      logInfo(s"CASCADE drop all database objects in $dbName: $allTableNames")
      val allTables = allTableNames.flatMap(
        table => snappyExternalCatalog.getTableIfExists(dbName, formatTableName(table)))
      // keep dropping leaves until empty
      if (allTables.nonEmpty) {
        // drop streams at the end
        val (streams, others) = allTables.partition(getTableType(_) == CatalogObjectType.Stream)
        var tables = others
        while (tables.nonEmpty) {
          val (leaves, remaining) = tables.partition(t => t.tableType == CatalogTableType.VIEW ||
              snappyExternalCatalog.getDependents(t.database, t.identifier.table, t,
                Nil, CatalogObjectType.Policy :: Nil).isEmpty)
          leaves.foreach(t => snappySession.dropTable(t.identifier, ifExists = true,
            t.tableType == CatalogTableType.VIEW))
          tables = remaining
        }
        if (streams.nonEmpty) {
          streams.foreach(s => snappySession.dropTable(
            s.identifier, ifExists = true, isView = false))
        }
      }

      // drop all the functions
      val allFunctions = listFunctions(dbName)
      if (allFunctions.nonEmpty) {
        allFunctions.foreach(f => dropFunction(f._1, ignoreIfNotExists = true))
      }
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    val dbName = formatDatabaseName(db)
    if (!ignoreIfNotExists && !databaseExists(dbName)) {
      throw SnappyExternalCatalog.databaseNotFoundException(db)
    }
    // user cannot drop own database
    if (dbName == defaultDbName) {
      throw new AnalysisException(s"Cannot drop own database $dbName")
    }
    validateDbName(formatDatabaseName(dbName), checkForDefault = true)

    // database/schema might exist in only one of the two catalogs so use ignoreIfNotExists=true
    // for both (exists check above ensures it should be present at least in one)
    dropAllDatabaseObjects(dbName, ignoreIfNotExists = true, cascade)

    super.dropDatabase(dbName, ignoreIfNotExists = true, cascade)

    // drop the database from store (no cascade required since catalog drop will take care)
    val conn = snappySession.defaultPooledConnection(db)
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(s"""DROP SCHEMA IF EXISTS "${Utils.toUpperCase(db)}" RESTRICT""")
      stmt.close()
    } finally {
      conn.close()
    }

    if (snappySession.enableHiveSupport) {
      hiveSessionCatalog.dropDatabase(db, ignoreIfNotExists = true, cascade)
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val dbName = formatDatabaseName(dbDefinition.name)
    if (!databaseExists(dbName)) {
      throw SnappyExternalCatalog.databaseNotFoundException(dbName)
    }
    if (super.databaseExists(dbName)) super.alterDatabase(dbDefinition)
    // also alter in hive catalog if present
    if (snappySession.enableHiveSupport &&
        hiveSessionCatalog.databaseExists(dbDefinition.name)) {
      hiveSessionCatalog.alterDatabase(dbDefinition)
    }
  }

  override def setCurrentDatabase(db: String): Unit =
    setCurrentDatabase(formatDatabaseName(db), force = false)

  /**
   * Identical to [[setCurrentDatabase]] but assumes that the passed name
   * has already been formatted by a call to [[formatDatabaseName]].
   */
  private[sql] def setCurrentDatabase(dbName: String, force: Boolean): Unit = {
    if (force || dbName != getCurrentDatabase) {
      validateDbName(dbName, checkForDefault = false)
      super.setCurrentDatabase(dbName)
      snappyExternalCatalog.setCurrentDatabase(dbName)
      // no need to set the current database in external hive metastore since the
      // database may not exist and all calls to it will already ensure fully qualified
      // table names
    }
  }

  override def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = formatDatabaseName(db)
    val externalCatalog = snappyExternalCatalog
    if (externalCatalog.databaseExists(dbName)) externalCatalog.getDatabase(dbName)
    else if (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(db)) {
      hiveSessionCatalog.getDatabaseMetadata(db)
    } else throw SnappyExternalCatalog.databaseNotFoundException(db)
  }

  override def databaseExists(db: String): Boolean = {
    super.databaseExists(db) ||
        (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(db))
  }

  override def listDatabases(): Seq[String] = synchronized {
    if (skipDefaultDatabases) {
      listAllDatabases().filter(s =>
        !s.equalsIgnoreCase(SnappyExternalCatalog.SPARK_DEFAULT_DATABASE) &&
            !s.equalsIgnoreCase(SnappyExternalCatalog.SYS_DATABASE) &&
            !s.equalsIgnoreCase(defaultDbName))
    } else listAllDatabases()
  }

  private def listAllDatabases(): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      (super.listDatabases() ++
          hiveSessionCatalog.listDatabases()).distinct.sorted
    } else super.listDatabases().distinct.sorted
  }

  override def listDatabases(pattern: String): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      (super.listDatabases(pattern) ++
          hiveSessionCatalog.listDatabases(pattern)).distinct.sorted
    } else super.listDatabases(pattern).distinct.sorted
  }

  protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit

  protected final def createTableImpl(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    // first check required permission to create objects in a database
    val dbName = getDbName(table.identifier)
    val tableName = formatTableName(table.identifier.table)
    checkDbPermission(dbName, tableName, defaultUser = null)

    // hive tables will be created in external hive catalog if enabled else will fail
    table.provider match {
      case Some(p) if p.equalsIgnoreCase(DDLUtils.HIVE_PROVIDER) =>
        if (snappySession.enableHiveSupport) {

          // check for existing table else for hive table it could create in both catalogs
          if (!ignoreIfExists && super.tableExists(table.identifier)) {
            val objectType = CatalogObjectType.getTableType(table)
            if (CatalogObjectType.isTableOrView(objectType)) {
              throw new TableAlreadyExistsException(db = dbName, table = tableName)
            } else {
              throw SnappyExternalCatalog.objectExistsException(table.identifier, objectType)
            }
          }

          // resolve table fully as per current database in this session
          internals.createTable(hiveSessionCatalog, resolveCatalogTable(table, dbName),
            ignoreIfExists, validateTableLocation)
        } else {
          throw Utils.analysisException(
            s"External hive support (${StaticSQLConf.CATALOG_IMPLEMENTATION.key} = hive) " +
                "is required to create hive tables")

        }
      case _ =>
        createDatabase(dbName, ignoreIfExists = true)
        // hack to always pass ignoreIfExists as true so that
        // for the case of CTAS for builtin tables which is handled
        // in SnappyHiveExternalCatalog but premature exception gets
        // thrown in newer SessionCatalog.createTable
        SnappyHiveExternalCatalog.ignoreIfExists.set(ignoreIfExists)
        try {
          baseCreateTable(table, ignoreIfExists = true, validateTableLocation)
        } finally {
          SnappyHiveExternalCatalog.ignoreIfExists.remove()
        }
    }

    contextFunctions.postCreateTable(table)
  }

  /**
   * Create catalog object for a BaseRelation backed by a Region in store or GemFire.
   *
   * This method is to be used for pre-entry into the catalog during a CTAS execution
   * for the inserts to proceed (which themselves may require the catalog entry
   * on executors). The GemFire provider uses it in a special way to update
   * the options stored for the catalog entry.
   */
  private[sql] def createTableForBuiltin(fullTableName: String, provider: String,
      schema: StructType, options: Map[String, String], ignoreIfExists: Boolean): Unit = {
    assert(CatalogObjectType.isTableBackedByRegion(SnappyContext.getProviderType(provider)) ||
        CatalogObjectType.isGemFireProvider(provider))
    val (dbName, tableName) = getTableWithDatabase(fullTableName, getCurrentDatabase)
    assert(dbName.length > 0)
    val catalogTable = CatalogTable(new TableIdentifier(tableName, Some(dbName)),
      CatalogTableType.EXTERNAL, DataSource.buildStorageFormatFromOptions(
        options + (DBTABLE_PROPERTY -> fullTableName)), schema, Some(provider))
    createTableImpl(catalogTable, ignoreIfExists, validateTableLocation = false)
  }

  protected def convertCharTypes(table: CatalogTable): CatalogTable = {
    if (convertCharTypesInMetadata) table.copy(schema = StructType(table.schema.map { field =>
      field.dataType match {
        case StringType if field.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) =>
          val md = field.metadata
          md.getString(Constant.CHAR_TYPE_BASE_PROP) match {
            case "CHAR" =>
              field.copy(dataType = CharType(md.getLong(Constant.CHAR_TYPE_SIZE_PROP).toInt))
            case "VARCHAR" =>
              field.copy(dataType = VarcharType(md.getLong(Constant.CHAR_TYPE_SIZE_PROP).toInt))
            case _ => field
          }
        case _ => field
      }
    })) else table
  }

  override def tableExists(name: TableIdentifier): Boolean = {
    if (super.tableExists(name)) true
    else if (snappySession.enableHiveSupport) {
      val dbName = getDbName(name)
      hiveSessionCatalog.databaseExists(dbName) &&
          hiveSessionCatalog.tableExists(qualifiedTableIdentifier(name, dbName))
    } else false
  }

  def getTableMetadataIfExists(name: TableIdentifier): Option[CatalogTable] = {
    try {
      Some(convertCharTypes(super.getTableMetadata(name)))
    } catch {
      case _: Exception =>
        val dbName = getDbName(name)
        if (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(dbName)) {
          try {
            Some(hiveSessionCatalog.getTableMetadata(qualifiedTableIdentifier(name, dbName)))
          } catch {
            case _: Exception => None
          }
        } else None
    }
  }

  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    getTableMetadataIfExists(name) match {
      case Some(table) => table
      case None => throw new TableNotFoundException(getDbName(name), name.table)
    }
  }

  override def dropTable(tableIdent: TableIdentifier, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val name = addMissingGlobalTempDb(tableIdent)

    if (isTemporaryTable(name)) {
      dropTemporaryTable(name)
    } else {
      val db = getDbName(name)
      val table = formatTableName(name.table)
      checkDbPermission(db, table, defaultUser = null)
      // resolve the table and destroy underlying storage if possible
      snappyExternalCatalog.getTableIfExists(db, table) match {
        case None =>
          // check in external hive catalog
          if (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(db)) {
            hiveSessionCatalog.dropTable(qualifiedTableIdentifier(name, db),
              ignoreIfNotExists, purge)
            return
          }
          if (ignoreIfNotExists) return else throw new TableNotFoundException(db, table)
        case Some(metadata) =>
          // fail if there are any existing dependents except policies
          val dependents = snappyExternalCatalog.getDependents(db, table,
            snappyExternalCatalog.getTable(db, table), Nil, CatalogObjectType.Policy :: Nil)
          if (dependents.nonEmpty) {
            throw new AnalysisException(s"Object $db.$table cannot be dropped because of " +
                s"dependent objects: ${dependents.map(_.identifier.unquotedString).mkString(",")}")
          }
          // remove from temporary base table if applicable
          dropFromTemporaryBaseTable(metadata)
          metadata.provider match {
            case Some(provider) if !provider.equalsIgnoreCase(DDLUtils.HIVE_PROVIDER) =>
              try {
                DataSource(snappySession, provider, userSpecifiedSchema = Some(metadata.schema),
                  partitionColumns = metadata.partitionColumnNames,
                  bucketSpec = metadata.bucketSpec,
                  options = metadata.storage.properties).resolveRelation() match {
                  case d: DestroyRelation if d ne null => d.destroy(ignoreIfNotExists)
                  case _ =>
                }
              } catch {
                case NonFatal(_) => // ignore any exception in class lookup
              }
            case _ =>
          }
      }
    }
    super.dropTable(name, ignoreIfNotExists, purge)
  }

  def addSampleDataFrame(base: LogicalPlan, sample: LogicalPlan, name: String = ""): Unit =
    contextFunctions.addSampleDataFrame(base, sample, name)

  /**
   * Return the set of temporary samples for a given table that are not tracked in catalog.
   */
  def getSamples(base: LogicalPlan): Seq[LogicalPlan] = contextFunctions.getSamples(base)

  /**
   * Return the set of samples for a given table that are tracked in catalog and are not temporary.
   */
  def getSampleRelations(baseTable: TableIdentifier): Seq[(LogicalPlan, String)] =
    contextFunctions.getSampleRelations(baseTable)

  protected def dropTemporaryTable(tableIdent: TableIdentifier): Unit =
    contextFunctions.dropTemporaryTable(tableIdent)

  protected def dropFromTemporaryBaseTable(table: CatalogTable): Unit =
    contextFunctions.dropFromTemporaryBaseTable(table)

  def lookupTopK(topKName: String): Option[(AnyRef, RDD[(Int, TopK)])] =
    contextFunctions.lookupTopK(topKName)

  def registerTopK(topK: AnyRef, rdd: RDD[(Int, TopK)],
      ifExists: Boolean, overwrite: Boolean): Boolean =
    contextFunctions.registerTopK(topK, rdd, ifExists, overwrite)

  def unregisterTopK(topKName: String): Unit = contextFunctions.unregisterTopK(topKName)

  override def alterTable(table: CatalogTable): Unit = {
    // first check required permission to alter objects in a database
    val dbName = getDbName(table.identifier)
    checkDbPermission(dbName, table.identifier.table, defaultUser = null)

    if (checkBuiltinCatalog(table.identifier)) super.alterTable(table)
    else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.alterTable(resolveCatalogTable(table, dbName))
    } else throw new TableNotFoundException(dbName, table.identifier.table)
  }

  override def alterTempViewDefinition(name: TableIdentifier,
      viewDefinition: LogicalPlan): Boolean = {
    super.alterTempViewDefinition(addMissingGlobalTempDb(name), viewDefinition)
  }

  override def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable =
    convertCharTypes(super.getTempViewOrPermanentTableMetadata(addMissingGlobalTempDb(name)))

  override def renameTable(old: TableIdentifier, newName: TableIdentifier): Unit = {
    val oldName = addMissingGlobalTempDb(old)
    if (isTemporaryTable(oldName)) {
      if (newName.database.isEmpty && oldName.database.contains(globalTempManager.database)) {
        super.renameTable(oldName, newName.copy(database = Some(globalTempManager.database)))
      } else super.renameTable(oldName, newName)
    } else {
      // first check required permission to alter objects in a database
      val oldDbName = getDbName(oldName)
      checkDbPermission(oldDbName, oldName.table, defaultUser = null)
      val newDbName = getDbName(newName)
      if (oldDbName != newDbName) {
        checkDbPermission(newDbName, newName.table, defaultUser = null)
      }

      if (checkBuiltinCatalog(oldName)) {
        getTableMetadataIfExists(oldName).flatMap(_.provider) match {
          // in-built tables don't support rename yet
          case Some(p) if SnappyContext.isBuiltInProvider(p) =>
            throw new UnsupportedOperationException(
              s"Table $oldName having provider '$p' does not support rename")
          case _ => super.renameTable(oldName, newName)
        }
      } else if (hiveSessionCatalog.databaseExists(oldDbName)) {
        hiveSessionCatalog.renameTable(qualifiedTableIdentifier(oldName, oldDbName),
          qualifiedTableIdentifier(newName, newDbName))
      } else throw new TableNotFoundException(oldDbName, oldName.table)
    }
  }

  override def loadTable(table: TableIdentifier, loadPath: String, isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = {
    // first check required permission to alter objects in a database
    val dbName = getDbName(table)
    checkDbPermission(dbName, table.table, defaultUser = null)

    if (checkBuiltinCatalog(table)) {
      super.loadTable(table, loadPath, isOverwrite, holdDDLTime)
    } else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.loadTable(qualifiedTableIdentifier(table, dbName),
        loadPath, isOverwrite, holdDDLTime)
    } else throw new TableNotFoundException(dbName, table.table)
  }

  def createPolicy(
      policyIdent: TableIdentifier,
      targetTable: TableIdentifier,
      policyFor: String,
      policyApplyTo: Seq[String],
      expandedPolicyApplyTo: Seq[String],
      currentUser: String,
      filterString: String): Unit = {

    // first check required permission to create objects in a database
    val dbName = getDbName(policyIdent)
    val policyName = formatTableName(policyIdent.table)
    val targetIdent = resolveTableIdentifier(targetTable)
    val targetDb = targetIdent.database.get
    // Target table database should be writable as well as own.
    // Owner of the target table database has full permissions on it so becomes
    // the policy owner too (can be an ldap group).
    val owner = checkDbPermission(targetDb, policyName, currentUser)
    if (targetDb != dbName) {
      checkDbPermission(dbName, policyName, currentUser)
    }
    createDatabase(dbName, ignoreIfExists = true)

    snappyExternalCatalog.createPolicy(dbName, policyName, targetIdent.unquotedString,
      policyFor, policyApplyTo, expandedPolicyApplyTo, owner, filterString)
  }

  private def getPolicyPlan(table: CatalogTable): LogicalPlan = {
    val filterExpression = table.properties.get(PolicyProperties.filterString) match {
      case Some(e) => parser.parseExpression(e)
      case None => throw new IllegalStateException("Filter for the policy not found")
    }
    val tableIdent = table.properties.get(PolicyProperties.targetTable) match {
      case Some(t) => snappySession.tableIdentifier(t)
      case None => throw new IllegalStateException("Target Table for the policy not found")
    }
    /* val targetRelation = lookupRelationImpl(tableIdent, None)
     val isTargetExternalRelation = targetRelation.find(x => x match {
       case _: ExternalRelation => true
       case _ => false
     }).isDefined
     */
    PolicyProperties.createFilterPlan(filterExpression, tableIdent,
      table.properties(PolicyProperties.policyOwner),
      table.properties(PolicyProperties.expandedPolicyApplyTo).split(',').
          toSeq.filterNot(_.isEmpty))
  }

  def newView(table: CatalogTable, child: LogicalPlan): LogicalPlan

  def newCatalogRelation(dbName: String, table: CatalogTable): LogicalPlan

  protected final def lookupRelationImpl(name: TableIdentifier, alias: Option[String],
      wrapped: Option[SnappySessionCatalog] = wrappedCatalog): LogicalPlan = wrapped match {
    case None => synchronized {
      val tableName = formatTableName(name.table)
      var view: Option[TableIdentifier] = Some(name)
      val relationPlan = (if (name.database.isEmpty) {
        getTempView(tableName) match {
          case None => globalTempManager.get(tableName)
          case s => s
        }
      } else None) match {
        case None =>
          val dbName =
            if (name.database.isEmpty) currentDb else formatDatabaseName(name.database.get)
          if (dbName == globalTempManager.database) {
            globalTempManager.get(tableName) match {
              case None => throw new TableNotFoundException(dbName, tableName)
              case Some(p) => p
            }
          } else {
            val table = snappyExternalCatalog.getTableIfExists(dbName, tableName) match {
              case None =>
                if (snappySession.enableHiveSupport) {
                  // lookupRelation uses HiveMetastoreCatalog that looks up the session state and
                  // catalog from the session every time so use withHiveState to switch the catalog
                  val state = snappySession.snappySessionState
                  if (hiveSessionCatalog.databaseExists(dbName)) state.withHiveSession {
                    return internals.lookupRelation(hiveSessionCatalog,
                      TableIdentifier(tableName, Some(dbName)), alias)
                  }
                }
                throw new TableNotFoundException(dbName, tableName)
              case Some(t) => t
            }
            if (table.tableType == CatalogTableType.VIEW) {
              if (table.viewText.isEmpty) sys.error("Invalid view without text.")
              newView(table, new SnappySqlParser(snappySession).parsePlan(table.viewText.get))
            } else if (CatalogObjectType.isPolicy(table)) {
              getPolicyPlan(table)
            } else {
              view = None
              newCatalogRelation(dbName, table)
            }
          }
        case Some(p) => p
      }
      internals.newSubqueryAlias(if (alias.isEmpty) tableName else alias.get, relationPlan, view)
    }

    case Some(c) => c.resolveRelationWithAlias(name, alias)
  }

  override def isTemporaryTable(name: TableIdentifier): Boolean = {
    if (name.database.isEmpty) synchronized {
      // check both local and global temporary tables
      val tableName = formatTableName(name.table)
      getTempView(tableName).isDefined || globalTempManager.get(tableName).isDefined
    } else if (formatDatabaseName(name.database.get) == globalTempManager.database) {
      globalTempManager.get(formatTableName(name.table)).isDefined
    } else false
  }

  override def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    if (dbName != globalTempManager.database && !databaseExists(dbName)) {
      throw SnappyExternalCatalog.databaseNotFoundException(db)
    }
    if (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(db)) {
      (super.listTables(db, pattern).toSet ++
          hiveSessionCatalog.listTables(db, pattern).toSet).toSeq
    } else super.listTables(db, pattern)
  }

  override def refreshTable(name: TableIdentifier): Unit = {
    val table = addMissingGlobalTempDb(name)
    super.refreshTable(table)
    if (!isTemporaryTable(table)) {
      val resolved = resolveTableIdentifier(table)
      snappyExternalCatalog.invalidate(resolved.database.get -> resolved.table)
      if (snappySession.enableHiveSupport) {
        hiveSessionCatalog.refreshTable(resolved)
      }
    }
  }

  def getDataSourceRelations[T](tableType: CatalogObjectType.Type): Seq[T] = {
    snappyExternalCatalog.getAllTables().collect {
      case table if tableType == CatalogObjectType.getTableType(table) =>
        resolveRelation(table.identifier).asInstanceOf[LogicalRelation].relation.asInstanceOf[T]
    }
  }

  private def toUrl(resource: FunctionResource): URL = {
    val path = resource.uri
    val uri = new Path(path).toUri
    if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
  }

  override def createPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    // first check required permission to create objects in a database
    val dbName = getDbName(tableName)
    checkDbPermission(dbName, tableName.table, defaultUser = null)

    if (checkBuiltinCatalog(tableName)) {
      super.createPartitions(tableName, parts, ignoreIfExists)
    } else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.createPartitions(qualifiedTableIdentifier(tableName, dbName),
        parts, ignoreIfExists)
    } else throw new TableNotFoundException(dbName, tableName.table)
  }

  override def dropPartitions(tableName: TableIdentifier, specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    // first check required permission to drop objects in a database
    val dbName = getDbName(tableName)
    checkDbPermission(dbName, tableName.table, defaultUser = null)

    if (checkBuiltinCatalog(tableName)) {
      super.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)
    } else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.dropPartitions(qualifiedTableIdentifier(tableName, dbName),
        specs, ignoreIfNotExists, purge, retainData)
    } else throw new TableNotFoundException(dbName, tableName.table)
  }

  override def alterPartitions(tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit = {
    // first check required permission to alter objects in a database
    val dbName = getDbName(tableName)
    checkDbPermission(dbName, tableName.table, defaultUser = null)

    if (checkBuiltinCatalog(tableName)) super.alterPartitions(tableName, parts)
    else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.alterPartitions(qualifiedTableIdentifier(tableName, dbName), parts)
    } else throw new TableNotFoundException(dbName, tableName.table)
  }

  override def renamePartitions(tableName: TableIdentifier, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    // first check required permission to alter objects in a database
    val dbName = getDbName(tableName)
    checkDbPermission(dbName, tableName.table, defaultUser = null)

    if (checkBuiltinCatalog(tableName)) {
      super.renamePartitions(tableName, specs, newSpecs)
    } else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.renamePartitions(qualifiedTableIdentifier(tableName, dbName),
        specs, newSpecs)
    } else throw new TableNotFoundException(dbName, tableName.table)
  }

  override def loadPartition(table: TableIdentifier, loadPath: String, spec: TablePartitionSpec,
      isOverwrite: Boolean, holdDDLTime: Boolean, inheritTableSpecs: Boolean): Unit = {
    // first check required permission to alter objects in a database
    val dbName = getDbName(table)
    checkDbPermission(dbName, table.table, defaultUser = null)

    if (checkBuiltinCatalog(table)) {
      super.loadPartition(table, loadPath, spec, isOverwrite, holdDDLTime, inheritTableSpecs)
    } else if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.loadPartition(qualifiedTableIdentifier(table, dbName),
        loadPath, spec, isOverwrite, holdDDLTime, inheritTableSpecs)
    } else throw new TableNotFoundException(dbName, table.table)
  }

  override def getPartition(table: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition = {
    if (checkBuiltinCatalog(table)) {
      return super.getPartition(table, spec)
    }
    val dbName = getDbName(table)
    if (hiveSessionCatalog.databaseExists(dbName)) {
      hiveSessionCatalog.getPartition(qualifiedTableIdentifier(table, dbName), spec)
    } else throw new TableNotFoundException(dbName, table.table)
  }

  override def listPartitionNames(tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      val dbName = getDbName(tableName)
      if (hiveSessionCatalog.databaseExists(dbName)) {
        return (super.listPartitionNames(tableName, partialSpec).toSet ++
            hiveSessionCatalog.listPartitionNames(qualifiedTableIdentifier(tableName, dbName),
              partialSpec).toSet).toSeq
      }
    }
    super.listPartitionNames(tableName, partialSpec)
  }

  override def listPartitions(tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    if (snappySession.enableHiveSupport) {
      val dbName = getDbName(tableName)
      if (hiveSessionCatalog.databaseExists(dbName)) {
        return (super.listPartitions(tableName, partialSpec).toSet ++
            hiveSessionCatalog.listPartitions(qualifiedTableIdentifier(tableName, dbName),
              partialSpec).toSet).toSeq
      }
    }
    super.listPartitions(tableName, partialSpec)
  }

  override def listPartitionsByFilter(tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    if (snappySession.enableHiveSupport) {
      val dbName = getDbName(tableName)
      if (hiveSessionCatalog.databaseExists(dbName)) {
        return (super.listPartitionsByFilter(tableName, predicates).toSet ++
            hiveSessionCatalog.listPartitionsByFilter(qualifiedTableIdentifier(
              tableName, dbName), predicates).toSet).toSeq
      }
    }
    super.listPartitionsByFilter(tableName, predicates)
  }

  // TODO: SW: clean up function resource loading to be like Spark with backward compatibility

  override def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    val qualifiedName = SnappyExternalCatalog.currentFunctionIdentifier.get()
    val functionQualifiedName = qualifiedName.unquotedString
    val parentLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
    val callbacks = ToolsCallbackInit.toolsCallback
    val newClassLoader = ContextJarUtils.getDriverJar(functionQualifiedName) match {
      case None =>
        val urls = if (callbacks ne null) {
          resources.map { r =>
            ContextJarUtils.fetchFile(functionQualifiedName, r.uri)
          }
        } else resources.map(toUrl)
        val newClassLoader = new MutableURLClassLoader(urls.toArray, parentLoader)
        ContextJarUtils.addDriverJar(functionQualifiedName, newClassLoader)
        newClassLoader

      case Some(c) => c
    }

    if (isEmbeddedMode) {
      callbacks.setSessionDependencies(snappySession.sparkContext,
        functionQualifiedName, newClassLoader, addAllJars = true)
    } else {
      newClassLoader.getURLs.foreach(url =>
        snappySession.sparkContext.addJar(url.getFile))
    }
  }

  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    // If the name itself is not qualified, add the current database to it.
    val dbName = getDbName(name)
    // first check required permission to create objects in a database
    checkDbPermission(dbName, name.funcName, defaultUser = null)

    val qualifiedName = name.copy(database = Some(dbName))
    ContextJarUtils.removeFunctionArtifacts(snappyExternalCatalog, Option(this),
      qualifiedName.database.get, qualifiedName.funcName, isEmbeddedMode, ignoreIfNotExists)
    super.dropFunction(name, ignoreIfNotExists)
  }

  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val dbName = getDbName(funcDefinition.identifier)
    // first check required permission to create objects in a database
    checkDbPermission(dbName, funcDefinition.identifier.funcName, defaultUser = null)
    createDatabase(dbName, ignoreIfExists = true)

    super.createFunction(funcDefinition, ignoreIfExists)

    if (isEmbeddedMode) {
      ContextJarUtils.addFunctionArtifacts(funcDefinition, dbName)
    }
  }

  private def isEmbeddedMode: Boolean = {
    SnappyContext.getClusterMode(snappySession.sparkContext) match {
      case SnappyEmbeddedMode(_, _) => true
      case _ => false
    }
  }

  override def functionExists(name: FunctionIdentifier): Boolean = {
    if (super.functionExists(name)) true
    else if (snappySession.enableHiveSupport) {
      val dbName = getDbName(name)
      if (hiveSessionCatalog.databaseExists(dbName)) {
        hiveSessionCatalog.functionExists(name.copy(database = Some(dbName)))
      } else false
    } else false
  }

  protected def makeFunctionBuilderImpl(funcName: String, className: String): FunctionBuilder = {
    val urlClassLoader = ContextJarUtils.getDriverJar(funcName) match {
      case None => org.apache.spark.util.Utils.getContextOrSparkClassLoader
      case Some(c) => c
    }
    val splitIndex = className.lastIndexOf("__")
    val actualClassName = if (splitIndex != -1) className.substring(0, splitIndex) else className
    val typeName = if (splitIndex != -1) className.substring(splitIndex + 2) else ""
    val dataType = if (typeName.isEmpty) None else Some(parser.parseDataType(typeName))
    UDFFunction.makeFunctionBuilder(funcName, urlClassLoader.loadClass(actualClassName), dataType)
  }

  /**
   * Return an [[Expression]] that represents the specified function, assuming it exists.
   *
   * For a temporary function or a permanent function that has been loaded,
   * this method will simply lookup the function through the
   * FunctionRegistry and create an expression based on the builder.
   *
   * For a permanent function that has not been loaded, we will first fetch its metadata
   * from the underlying external catalog. Then, we will load all resources associated
   * with this function (i.e. jars and files). Finally, we create a function builder
   * based on the function class and put the builder into the FunctionRegistry.
   * The name of this function in the FunctionRegistry will be `databaseName.functionName`.
   */
  override def lookupFunction(name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    // If the name itself is not qualified, add the current database to it.
    val dbName = getDbName(name)
    val qualifiedName = name.copy(database = Some(dbName))
    // for some reason Spark's lookup uses current database rather than that of the function
    val currentDatabase = currentDb
    currentDb = dbName
    SnappyExternalCatalog.currentFunctionIdentifier.set(qualifiedName)
    try {
      super.lookupFunction(name, children)
    } catch {
      case _: NoSuchFunctionException if snappySession.enableHiveSupport &&
          hiveSessionCatalog.databaseExists(dbName) =>
        // lookup in external hive catalog
        hiveSessionCatalog.lookupFunction(qualifiedName, children)
    } finally {
      SnappyExternalCatalog.currentFunctionIdentifier.set(null)
      currentDb = currentDatabase
    }
  }

  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = {
    // If the name itself is not qualified, add the current database to it.
    val dbName = getDbName(name)
    // for some reason Spark's lookup uses current database rather than that of the function
    val currentDatabase = currentDb
    currentDb = dbName
    try {
      super.lookupFunctionInfo(name)
    } catch {
      case _: NoSuchFunctionException if snappySession.enableHiveSupport &&
          hiveSessionCatalog.databaseExists(dbName) =>
        // lookup in external hive catalog
        hiveSessionCatalog.lookupFunctionInfo(name.copy(database = Some(dbName)))
    } finally {
      currentDb = currentDatabase
    }
  }

  override def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    try {
      super.getFunctionMetadata(name)
    } catch {
      case e: NoSuchFunctionException if snappySession.enableHiveSupport =>
        // lookup in external hive catalog
        val dbName = getDbName(name)
        if (hiveSessionCatalog.databaseExists(dbName)) {
          hiveSessionCatalog.getFunctionMetadata(name.copy(database = Some(dbName)))
        } else throw e
    }
  }

  override def listFunctions(db: String,
      pattern: String): Seq[(FunctionIdentifier, String)] = {
    if (snappySession.enableHiveSupport && hiveSessionCatalog.databaseExists(db)) {
      (super.listFunctions(db, pattern).toSet ++
          hiveSessionCatalog.listFunctions(db, pattern).toSet).toSeq
    } else super.listFunctions(db, pattern)
  }

  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Test only method
   */
  def destroyAndRegisterBuiltInFunctionsForTests(): Unit = {
    functionRegistry.clear()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  override def reset(): Unit = synchronized {
    // flag to avoid listing the DEFAULT and SYS databases to avoid attempting to drop them
    skipDefaultDatabases = true
    try {
      super.reset()
      if (snappySession.enableHiveSupport) hiveSessionCatalog.reset()
    } finally {
      skipDefaultDatabases = false
    }
  }
}
