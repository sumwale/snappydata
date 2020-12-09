/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.hive

import java.lang.reflect.InvocationTargetException
import javax.annotation.concurrent.GuardedBy

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionException

import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.internal.cache.{LocalRegion, PartitionedRegion}
import com.gemstone.gemfire.internal.{GFToSlf4jBridge, LogWriterImpl}
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import com.pivotal.gemfirexd.Constants
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.engine.diag.SysVTIs
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary
import io.snappydata.sql.catalog.SnappyExternalCatalog._
import io.snappydata.sql.catalog.{CatalogObjectType, ConnectorExternalCatalog, RelationInfo, SnappyExternalCatalog}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.SparkConf
import org.apache.spark.jdbc.{ConnectionConf, ConnectionUtil}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils.EMPTY_STRING_ARRAY
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.sources.JdbcExtendedUtils.normalizeSchema
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.LongType

abstract class SnappyHiveExternalCatalog(val conf: SparkConf,
    val hadoopConf: Configuration, val createTime: Long)
    extends SnappyHiveCatalogBase(conf, hadoopConf) with SnappyExternalCatalog {

  /** A cache of Spark SQL data source tables that have been accessed. */
  // noinspection UnstableApiUsage
  protected final val cachedCatalogTables: LoadingCache[(String, String), CatalogTable] = {

    // base initialization first

    // fire dummy queries to initialize more components of hive meta-store
    withHiveExceptionHandling {
      assert(!client.tableExists(SYS_DATABASE, "dbs"))
      assert(!client.functionExists(SYS_DATABASE, "funcs"))
    }

    // initialize the CacheLoader

    val cacheLoader = new CacheLoader[(String, String), CatalogTable]() {
      override def load(name: (String, String)): CatalogTable = {
        logDebug(s"Looking up data source for ${name._1}.${name._2}")
        withHiveExceptionHandling {
          try {
            finalizeCatalogTable(SnappyHiveExternalCatalog.super.getTable(name._1, name._2))
          } catch {
            case _: NoSuchTableException =>
              nonExistentTables.put(name, java.lang.Boolean.TRUE)
              throw new TableNotFoundException(name._1, name._2)
            case _: NullPointerException =>
              // dropTableUnsafe() searches for below exception message. check before changing.
              throw new AnalysisException(
                s"Table ${name._1}.${name._2} might be inconsistent in hive catalog. " +
                    "Use system procedure SYS.REMOVE_METASTORE_ENTRY to remove inconsistency. " +
                    "Refer to troubleshooting section of documentation for more details")
          }
        }
      }
    }
    CacheBuilder.newBuilder().maximumSize(ConnectorExternalCatalog.cacheSize).build(cacheLoader)
  }

  /** A cache of SQL data source tables that are missing in catalog. */
  // noinspection UnstableApiUsage
  protected final val nonExistentTables: Cache[(String, String), java.lang.Boolean] = {
    CacheBuilder.newBuilder().maximumSize(ConnectorExternalCatalog.cacheSize).build()
  }

  @tailrec
  private def isDisconnectException(t: Throwable): Boolean = {
    if (t ne null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          // NPE can be seen if catalog object is being dropped concurrently
          t.isInstanceOf[NullPointerException] ||
          tClass.contains("DisconnectException") ||
          (tClass.contains("MetaException") && t.getMessage.contains("retries")) ||
          isDisconnectException(t.getCause)
    } else {
      false
    }
  }

  /**
   * Retries on transient disconnect exceptions.
   */
  protected[sql] def withHiveExceptionHandling[T](function: => T): T = synchronized {
    val skipFlags = GfxdDataDictionary.SKIP_CATALOG_OPS.get()
    val oldSkipCatalogCalls = skipFlags.skipHiveCatalogCalls
    val oldSkipLocks = skipFlags.skipDDLocks
    skipFlags.skipHiveCatalogCalls = true
    skipFlags.skipDDLocks = true
    try {
      function
    } catch {
      case he: Exception if isDisconnectException(he) =>
        // stale JDBC connection
        closeHive(clearCache = false)
        suspendActiveSession {
          hiveClient = hiveClient.newSession()
        }
        function
      case e: InvocationTargetException =>
        if (e.getCause ne null) throw e.getCause else throw e
      case e: ExecutionException =>
        if (e.getCause ne null) throw e.getCause else throw e
    } finally {
      skipFlags.skipDDLocks = oldSkipLocks
      skipFlags.skipHiveCatalogCalls = oldSkipCatalogCalls
    }
  }

  def getCatalogSchemaVersion: Long = {
    // schema version is now always stored in the profile to be able to exchange with other
    // nodes and update from incoming nodes easily
    GemFireXDUtils.getMyProfile(true).getCatalogSchemaVersion
  }

  def registerCatalogSchemaChange(relations: Seq[(String, String)]): Unit = {
    GemFireXDUtils.getMyProfile(true).incrementCatalogSchemaVersion()
    invalidateCaches(relations)
  }

  override def invalidateCaches(relations: Seq[(String, String)]): Unit = {
    RefreshMetadata.executeOnAll(SnappyContext.globalSparkContext,
      RefreshMetadata.UPDATE_CATALOG_SCHEMA_VERSION, getCatalogSchemaVersion -> relations,
      executeInConnector = false)
  }

  override def invalidate(name: (String, String)): Unit = {
    cachedCatalogTables.invalidate(name)
    nonExistentTables.invalidate(name)
    // also clear "isRowBuffer" since it may have been cached incorrectly
    // when column store was still being created
    Misc.getRegion(Misc.getRegionPath(JdbcExtendedUtils.toUpperCase(name._1),
      JdbcExtendedUtils.toUpperCase(name._2), null), false, true)
        .asInstanceOf[LocalRegion] match {
      case pr: PartitionedRegion => pr.clearIsRowBuffer()
      case _ =>
    }
  }

  override def invalidateAll(): Unit = {
    cachedCatalogTables.invalidateAll()
    nonExistentTables.invalidateAll()
    // also clear "isRowBuffer" for all regions
    for (pr <- PartitionedRegion.getAllPartitionedRegions.asScala) {
      pr.clearIsRowBuffer()
    }
  }

  // --------------------------------------------------------------------------
  // Base HiveExternalCatalog calls
  // --------------------------------------------------------------------------

  protected def baseCreateDatabase(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit

  protected def baseDropDatabase(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit

  protected def baseCreateTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit

  protected def baseDropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit

  protected def baseAlterTable(table: CatalogTable): Unit

  protected def baseRenameTable(db: String, oldName: String, newName: String): Unit

  protected def baseLoadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit

  protected def baseCreateFunction(db: String, funcDefinition: CatalogFunction): Unit

  protected def baseDropFunction(db: String, name: String): Unit

  protected def baseRenameFunction(db: String, oldName: String, newName: String): Unit

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  protected def createDatabaseImpl(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = {
    // dot is used for database, name separation and will cause many problems if present
    if (dbDefinition.name.indexOf('.') != -1) {
      throw new AnalysisException(
        s"Database/Schema '${dbDefinition.name}' cannot contain dot in its name")
    }
    // dependent tables are store comma separated so don't allow commas in database names
    if (dbDefinition.name.indexOf(',') != -1) {
      throw new AnalysisException(
        s"Database/Schema '${dbDefinition.name}' cannot contain comma in its name")
    }
    if (databaseExists(dbDefinition.name)) {
      if (ignoreIfExists) return
      else throw new AnalysisException(s"Database/Schema ${dbDefinition.name} already exists")
    }
    withHiveExceptionHandling(baseCreateDatabase(dbDefinition, ignoreIfExists))
  }

  protected def dropDatabaseImpl(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    if (db == SYS_DATABASE) {
      throw new AnalysisException(s"$db is a system preserved database/schema")
    }
    try {
      withHiveExceptionHandling(baseDropDatabase(db, ignoreIfNotExists, cascade))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.databaseNotFoundException(db)
    }
  }

  protected def alterDatabaseImpl(dbDefinition: CatalogDatabase): Unit = {
    try {
      withHiveExceptionHandling(super.alterDatabase(dbDefinition))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.databaseNotFoundException(dbDefinition.name)
    }
  }

  // Special in-built SYS database does not have hive catalog entry so the methods below
  // add that specifically to the existing databases.

  override def getDatabase(db: String): CatalogDatabase = {
    try {
      if (db == SYS_DATABASE) systemDatabaseDefinition
      else withHiveExceptionHandling(super.getDatabase(db).copy(name = db))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.databaseNotFoundException(db)
    }
  }

  override def databaseExists(db: String): Boolean = {
    if (db == SYS_DATABASE) true
    else {
      // if cache is small enough then linearly search in it since hive call is expensive
      if (cachedCatalogTables.size() <= 200) {
        val itr = cachedCatalogTables.asMap().keySet().iterator()
        while (itr.hasNext) {
          if (itr.next()._1 == db) return true
        }
      }
      withHiveExceptionHandling(super.databaseExists(db))
    }
  }

  override def listDatabases(): Seq[String] = {
    (withHiveExceptionHandling(super.listDatabases().toSet) + SYS_DATABASE)
        .toSeq.sorted
  }

  override def listDatabases(pattern: String): Seq[String] = {
    (withHiveExceptionHandling(super.listDatabases(pattern).toSet) ++
        StringUtils.filterPattern(Seq(SYS_DATABASE), pattern)).toSeq.sorted
  }

  override def setCurrentDatabase(db: String): Unit = {
    try {
      withHiveExceptionHandling(super.setCurrentDatabase(db))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.databaseNotFoundException(db)
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  private def addDependentToBase(name: (String, String), dependent: String): Unit = {
    withHiveExceptionHandling(client.getTableOption(name._1, name._2)) match {
      case None => // ignore, can be a temporary table
      case Some(baseTable) =>
        val dependents = SnappyExternalCatalog.getDependentsValue(baseTable.properties) match {
          case None => dependent
          case Some(deps) =>
            // add only if it doesn't exist
            if (deps.split(',').contains(dependent)) return else deps + "," + dependent
        }
        val newProps = baseTable.properties.filterNot(
          _._1.equalsIgnoreCase(DEPENDENT_RELATIONS))
        withHiveExceptionHandling(client.alterTable(baseTable.copy(properties =
            newProps + (DEPENDENT_RELATIONS -> dependents))))
    }
  }

  private def addViewProperties(tableDefinition: CatalogTable): CatalogTable = {
    // add split VIEW properties for large view strings
    var catalogTable = tableDefinition
    if (catalogTable.tableType == CatalogTableType.VIEW) {
      val maxLen = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
      var props: scala.collection.Map[String, String] = catalogTable.properties
      catalogTable.viewText match {
        case Some(v) if v.length > maxLen =>
          catalogTable = catalogTable.copy(viewText = Some(v.substring(0, maxLen)))
          props = JdbcExtendedUtils.addSplitProperty(v, SPLIT_VIEW_TEXT_PROPERTY, props, maxLen)
        case _ =>
      }
      internals.catalogTableViewOriginalText(catalogTable) match {
        case Some(v) if v.length > maxLen =>
          catalogTable = internals.newCatalogTableWithViewOriginalText(
            catalogTable, Some(v.substring(0, maxLen)))
          props = JdbcExtendedUtils.addSplitProperty(v, SPLIT_VIEW_ORIGINAL_TEXT_PROPERTY,
            props, maxLen)
        case _ =>
      }
      // add the schema as JSON string to restore with full properties
      props = JdbcExtendedUtils.addSplitProperty(catalogTable.schema.json,
        SnappyExternalCatalog.SPLIT_VIEW_SCHEMA, props, maxLen)
      catalogTable = catalogTable.copy(properties = props.toMap)
    }
    catalogTable
  }

  private def removeDependentFromBase(name: (String, String),
      dependent: String): Unit = withHiveExceptionHandling {
    withHiveExceptionHandling(client.getTableOption(name._1, name._2)) match {
      case None => // ignore, can be a temporary table
      case Some(baseTable) =>
        SnappyExternalCatalog.getDependents(baseTable.properties) match {
          case deps if deps.length > 0 =>
            // remove all instances in case there are multiple coming from older releases
            val dependents = deps.toSet
            if (dependents.contains(dependent)) withHiveExceptionHandling {
              val newProps = baseTable.properties.filterNot(
                _._1.equalsIgnoreCase(DEPENDENT_RELATIONS))
              if (dependents.size == 1) {
                client.alterTable(baseTable.copy(properties = newProps))
              } else {
                val newDependents = (dependents - dependent).mkString(",")
                client.alterTable(baseTable.copy(properties = newProps +
                    (DEPENDENT_RELATIONS -> newDependents)))
              }
            }
          case _ =>
        }
    }
  }

  protected def createTableImpl(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val catalogTable = addViewProperties(tableDefinition)
    var ifExists =
      if (ignoreIfExists) {
        val realIfExists = SnappyHiveExternalCatalog.ignoreIfExists.get()
        // check if the CTAS flag has been explicitly set else honour the passed flag
        (realIfExists eq null) || realIfExists.booleanValue()
      } else false
    // Add dependency on base table if required. This is done before actual table
    // entry so that if there is a cluster failure between the two steps, then
    // table will still not be in catalog and base table will simply ignore
    // the dependents not present during reads in getDependents.
    val refreshRelations = getTableWithBaseTable(catalogTable)
    if (refreshRelations.length > 1) {
      val dependent = catalogTable.identifier.unquotedString
      addDependentToBase(refreshRelations.head, dependent)
    }

    val isGemFireTable = catalogTable.provider.isDefined &&
        CatalogObjectType.isGemFireProvider(catalogTable.provider.get)
    // for tables with backing region using CTAS to create, the catalog entry has already been
    // made before to enable inserts, so alter it with the final definition here
    if (!ifExists && (CatalogObjectType.isTableBackedByRegion(
      CatalogObjectType.getTableType(catalogTable)) || isGemFireTable)) {
      withHiveExceptionHandling {
        val dbName = catalogTable.database
        val tableName = catalogTable.identifier.table
        if (client.tableExists(dbName, tableName)) {
          // This is the case of CTAS or GemFire. With CTAS ignoreIfExists is always false at this
          // point and any non-CTAS case with ignoreIfExists=false would have failed much earlier
          // when creating the backing region since table already exists for the Some(.) case.
          // Recreate the catalog table because final properties may be slightly different.
          if (isGemFireTable) ifExists = true
          else client.dropTable(dbName, tableName, ignoreIfNotExists = true, purge = false)
          invalidate(dbName -> tableName)
        }
      }
    }
    try {
      withHiveExceptionHandling(baseCreateTable(catalogTable, ifExists))
    } catch {
      case e: TableAlreadyExistsException =>
        val objectType = CatalogObjectType.getTableType(tableDefinition)
        if (CatalogObjectType.isTableOrView(objectType)) throw e
        else throw objectExistsException(tableDefinition.identifier, objectType)
    }

    // refresh cache for required tables
    registerCatalogSchemaChange(refreshRelations)
  }

  def dropTableUnsafe(db: String, table: String, forceDrop: Boolean): Unit = {
    try {
      super.getTable(db, table)
      // no exception raised while getting catalogTable
      if (forceDrop) {
        // parameter to force drop entry from metastore is set
        withHiveExceptionHandling(super.dropTable(db, table,
          ignoreIfNotExists = true, purge = true))
      } else {
        // AnalysisException not thrown while getting table. suspecting that wrong table
        // name is passed. throwing exception as a precaution.
        throw new AnalysisException("Table retrieved successfully. To " +
            "continue to drop this table change FORCE_DROP argument in procedure to true")
      }
    } catch {
      case a: AnalysisException if a.message.contains("might be inconsistent in hive catalog") =>
        // exception is expected as table might be inconsistent. continuing to drop
        withHiveExceptionHandling(super.dropTable(db, table,
          ignoreIfNotExists = true, purge = true))
    }

    SnappySession.clearAllCache(onlyQueryPlanCache = true)
    CodeGeneration.clearAllCache()
    invalidate(db -> table)
  }

  protected def dropTableImpl(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val tableDefinition = getTableIfExists(db, table) match {
      case None =>
        if (ignoreIfNotExists) return else throw new TableNotFoundException(db, table)
      case Some(t) => t
    }
    withHiveExceptionHandling(baseDropTable(db, table, ignoreIfNotExists, purge))

    // drop all policies for the table
    if (Misc.getMemStoreBooting.isRLSEnabled) {
      val policies = getPolicies(db, table, tableDefinition.properties)
      if (policies.nonEmpty) for (policy <- policies) {
        val dbName = policy.database
        val policyName = policy.identifier.table
        withHiveExceptionHandling(baseDropTable(dbName, policyName,
          ignoreIfNotExists = true, purge = false))
        invalidate(dbName -> policyName)
      }
    }

    // remove from base table if this is a dependent relation
    val refreshRelations = getTableWithBaseTable(tableDefinition)
    if (refreshRelations.length > 1) {
      val dependent = s"$db.$table"
      removeDependentFromBase(refreshRelations.head, dependent)
    }

    // refresh cache for required tables
    registerCatalogSchemaChange(refreshRelations)
  }

  protected def alterTableImpl(tableDefinition: CatalogTable): Unit = {
    val catalogTable = addViewProperties(tableDefinition)
    val dbName = catalogTable.database
    val tableName = catalogTable.identifier.table

    // if schema has changed then assume only that has to be changed and add the schema
    // property separately because Spark's HiveExternalCatalog does not support schema changes
    if (catalogTable.tableType == CatalogTableType.EXTERNAL) {
      val oldRawDefinition = withHiveExceptionHandling(client.getTable(
        catalogTable.database, catalogTable.identifier.table))
      val oldSchema = ExternalStoreUtils.getTableSchema(
        oldRawDefinition.properties, forView = false) match {
        case None => oldRawDefinition.schema
        case Some(s) => s
      }
      if (oldSchema.length != catalogTable.schema.length) {
        val maxLen = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
        // only change to new schema with corresponding properties and assume
        // rest is same since this can only come through SnappySession.alterTable
        val newProps = JdbcExtendedUtils.addSplitProperty(catalogTable.schema.json,
          SnappyExternalCatalog.TABLE_SCHEMA, oldRawDefinition.properties, maxLen)
        withHiveExceptionHandling(client.alterTable(oldRawDefinition.copy(
          schema = catalogTable.schema, properties = newProps.toMap)))

        registerCatalogSchemaChange(dbName -> tableName :: Nil)
        return
      }
    }

    withHiveExceptionHandling(baseAlterTable(catalogTable))

    registerCatalogSchemaChange(dbName -> tableName :: Nil)
  }

  protected def renameTableImpl(db: String, oldName: String, newName: String): Unit = {
    withHiveExceptionHandling(baseRenameTable(db, oldName, newName))

    registerCatalogSchemaChange(db -> oldName :: db -> newName :: Nil)
  }

  /**
   * Transform given CatalogTable to final form filling in viewText and other fields
   * using the properties if required.
   */
  protected def finalizeCatalogTable(table: CatalogTable): CatalogTable = {
    val tableIdent = table.identifier
    // VIEW text is stored as split text for large view strings,
    // so restore its full text and schema from properties if present
    val newTable = if (table.tableType == CatalogTableType.VIEW) {
      val viewText = JdbcExtendedUtils.readSplitProperty(SPLIT_VIEW_TEXT_PROPERTY,
        table.properties).orElse(table.viewText)
      val viewOriginalText = JdbcExtendedUtils.readSplitProperty(SPLIT_VIEW_ORIGINAL_TEXT_PROPERTY,
        table.properties).orElse(internals.catalogTableViewOriginalText(table))
      // update the meta-data from properties
      ExternalStoreUtils.getTableSchema(table.properties, forView = true) match {
        case Some(s) => internals.newCatalogTableWithViewOriginalText(
          table.copy(identifier = tableIdent, schema = s, viewText = viewText), viewOriginalText)
        case None => internals.newCatalogTableWithViewOriginalText(
          table.copy(identifier = tableIdent, viewText = viewText), viewOriginalText)
      }
    } else if (CatalogObjectType.isPolicy(table)) {
      // explicitly change table name in policy properties to lower-case
      // to deal with older releases that store the name in upper-case
      table.copy(identifier = tableIdent, schema = normalizeSchema(table.schema),
        properties = table.properties.updated(PolicyProperties.targetTable,
          JdbcExtendedUtils.toLowerCase(table.properties(PolicyProperties.targetTable))))
    } else table.provider match {
      case Some(provider) if SnappyContext.isBuiltInProvider(provider) ||
          CatalogObjectType.isGemFireProvider(provider) =>
        // add dbtable property which is not present in old releases
        val storageFormat =
          if (table.storage.properties.contains(DBTABLE_PROPERTY)) table.storage
          else {
            table.storage.copy(properties = table.storage.properties +
                (DBTABLE_PROPERTY -> tableIdent.unquotedString))
          }
        // schema is "normalized" to deal with upgrade from previous
        // releases that store column names in upper-case (SNAP-3090)
        table.copy(identifier = tableIdent, schema = normalizeSchema(table.schema),
          storage = storageFormat)
      case _ => table.copy(identifier = tableIdent)
    }
    // explicitly add weightage column to sample tables for old catalog data
    if (CatalogObjectType.getTableType(newTable) == CatalogObjectType.Sample &&
        !newTable.schema(table.schema.length - 1).name.equalsIgnoreCase(
          Utils.WEIGHTAGE_COLUMN_NAME)) {
      newTable.copy(schema = newTable.schema.add(Utils.WEIGHTAGE_COLUMN_NAME, LongType,
        nullable = false))
    } else newTable
  }

  override protected def getCachedCatalogTable(db: String, table: String): CatalogTable = {
    val name = db -> table
    if (nonExistentTables.getIfPresent(name) eq java.lang.Boolean.TRUE) {
      throw new TableNotFoundException(db, table)
    }
    // need to do the load under a sync block to avoid deadlock due to lock inversion
    // (sync block and map loader future) so do a get separately first
    val catalogTable = cachedCatalogTables.getIfPresent(name)
    if (catalogTable ne null) catalogTable
    else withHiveExceptionHandling(cachedCatalogTables.get(name))
  }

  private def toLowerCase(s: Array[String]): Array[String] = {
    val r = new Array[String](s.length)
    for (i <- s.indices) {
      r(i) = JdbcExtendedUtils.toLowerCase(s(i))
    }
    r
  }

  override def getRelationInfo(db: String, table: String,
      rowTable: Boolean): (RelationInfo, Option[LocalRegion]) = {
    if (SYS_DATABASE.equalsIgnoreCase(db)) {
      RelationInfo(1, isPartitioned = false) -> None
    } else {
      val r = Misc.getRegion(Misc.getRegionPath(JdbcExtendedUtils.toUpperCase(db),
        JdbcExtendedUtils.toUpperCase(table), null),
        true, false).asInstanceOf[LocalRegion]
      val indexCols = if (rowTable) {
        toLowerCase(GfxdSystemProcedures.getIndexColumns(r).asScala.toArray)
      } else EMPTY_STRING_ARRAY
      val pkCols = if (rowTable) {
        toLowerCase(GfxdSystemProcedures.getPKColumns(r).asScala.toArray)
      } else EMPTY_STRING_ARRAY
      r match {
        case pr: PartitionedRegion =>
          val resolver = pr.getPartitionResolver.asInstanceOf[GfxdPartitionByExpressionResolver]
          val partCols = toLowerCase(resolver.getColumnNames)
          RelationInfo(pr.getTotalNumberOfBuckets, isPartitioned = true, partCols,
            indexCols, pkCols) -> Some(pr)
        case _ => RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY,
          indexCols, pkCols) -> Some(r)
      }
    }
  }

  override def createPolicy(dbName: String, policyName: String, targetTable: String,
      policyFor: String, policyApplyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      owner: String, filterString: String): Unit = {

    val policyProperties = new mutable.HashMap[String, String]
    policyProperties.put(PolicyProperties.targetTable, targetTable)
    policyProperties.put(PolicyProperties.filterString, filterString)
    policyProperties.put(PolicyProperties.policyFor, policyFor)
    policyProperties.put(PolicyProperties.policyApplyTo, policyApplyTo.mkString(","))
    policyProperties.put(PolicyProperties.expandedPolicyApplyTo,
      expandedPolicyApplyTo.mkString(","))
    policyProperties.put(PolicyProperties.policyOwner, owner)
    val tableDefinition = CatalogTable(
      identifier = TableIdentifier(policyName, Some(dbName)),
      tableType = CatalogTableType.EXTERNAL,
      schema = JdbcExtendedUtils.EMPTY_SCHEMA,
      provider = Some("policy"),
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map(BASETABLE_PROPERTY -> targetTable)
      ),
      properties = policyProperties.toMap)

    createTable(tableDefinition, ignoreIfExists = false)
  }

  def refreshPolicies(ldapGroup: String): Unit = {
    val qualifiedLdapGroup = Constants.LDAP_GROUP_PREFIX + ldapGroup
    getAllTables().filter(_.provider.exists(_.equalsIgnoreCase("policy"))).foreach { table =>
      val applyToStr = table.properties(PolicyProperties.policyApplyTo)
      if (applyToStr.nonEmpty) {
        val applyTo = applyToStr.split(',')
        if (applyTo.contains(qualifiedLdapGroup)) {
          val expandedApplyTo = ExternalStoreUtils.getExpandedGranteesIterator(applyTo).toSeq
          val newProperties = table.properties +
              (PolicyProperties.expandedPolicyApplyTo -> expandedApplyTo.mkString(","))
          withHiveExceptionHandling(baseAlterTable(table.copy(properties = newProperties)))
        }
      }
    }
  }

  override def tableExists(db: String, table: String): Boolean = {
    try {
      getTable(db, table) ne null
    } catch {
      case _: NoSuchTableException => false
    }
  }

  override def listTables(db: String): Seq[String] = {
    if (SYS_DATABASE.equalsIgnoreCase(db)) listTables(db, "*")
    else withHiveExceptionHandling(super.listTables(db))
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    if (SYS_DATABASE.equalsIgnoreCase(db)) {
      // check for a system table/VTI in store
      val session = SparkSession.getActiveSession
      val conn = ConnectionUtil.getPooledConnection(db, new ConnectionConf(
        ExternalStoreUtils.validateAndGetAllProps(session, ExternalStoreUtils.emptyCIMutableMap)))
      try {
        // hive compatible filter patterns are different from JDBC ones
        // so get all tables in the database and apply filter separately
        val rs = conn.getMetaData.getTables(null, JdbcExtendedUtils.toUpperCase(db), "%", null)
        val buffer = new mutable.ArrayBuffer[String]()
        // add special case sys.members which is a distributed VTI but used by
        // SnappyData layer as a replicated one
        buffer += MEMBERS_VTI
        while (rs.next()) {
          // skip distributed VTIs
          if (rs.getString(4) != SysVTIs.LOCAL_VTI) {
            buffer += JdbcExtendedUtils.toLowerCase(rs.getString(3))
          }
        }
        rs.close()
        if (pattern == "*") buffer else StringUtils.filterPattern(buffer, pattern)
      } finally {
        conn.close()
      }
    } else withHiveExceptionHandling(super.listTables(db, pattern))
  }

  override def loadTable(db: String, table: String, loadPath: String,
      isOverwrite: Boolean, holdDDLTime: Boolean): Unit = {
    withHiveExceptionHandling(super.loadTable(db, table, loadPath, isOverwrite, holdDDLTime))
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    withHiveExceptionHandling(super.createPartitions(db, table, parts, ignoreIfExists))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    withHiveExceptionHandling(super.dropPartitions(db, table, parts, ignoreIfNotExists,
      purge, retainData))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    withHiveExceptionHandling(super.renamePartitions(db, table, specs, newSpecs))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def alterPartitions(db: String, table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    withHiveExceptionHandling(super.alterPartitions(db, table, parts))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def loadPartition(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, isOverwrite: Boolean, holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    withHiveExceptionHandling(super.loadPartition(db, table, loadPath, partition,
      isOverwrite, holdDDLTime, inheritTableSpecs))
  }

  protected def loadDynamicPartitionsImpl(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    withHiveExceptionHandling(baseLoadDynamicPartitions(db, table, loadPath, partition,
      replace, numDP, holdDDLTime))
  }

  override def getPartition(db: String, table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    withHiveExceptionHandling(super.getPartition(db, table, spec))
  }

  override def getPartitionOption(db: String, table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    withHiveExceptionHandling(super.getPartitionOption(db, table, spec))
  }

  override def listPartitionNames(db: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    withHiveExceptionHandling(super.listPartitionNames(db, table, partialSpec))
  }

  override def listPartitions(db: String, table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitions(db, table, partialSpec))
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  protected def createFunctionImpl(db: String, funcDefinition: CatalogFunction): Unit = {
    withHiveExceptionHandling(baseCreateFunction(db, funcDefinition))
    SnappySession.clearAllCache()
  }

  protected def dropFunctionImpl(db: String, name: String): Unit = {
    withHiveExceptionHandling(baseDropFunction(db, name))
    SnappySession.clearAllCache()
  }

  protected def renameFunctionImpl(db: String, oldName: String, newName: String): Unit = {
    withHiveExceptionHandling(baseRenameFunction(db, oldName, newName))
    SnappySession.clearAllCache()
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    withHiveExceptionHandling(super.getFunction(db, funcName))
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    withHiveExceptionHandling(super.functionExists(db, funcName))
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    withHiveExceptionHandling(super.listFunctions(db, pattern))
  }

  /**
   * Suspend the active SparkSession in case "function" creates new threads
   * that can end up inheriting it. Currently used during hive client creation
   * otherwise the BoneCP background threads hold on to old sessions
   * (even after a restart) due to the InheritableThreadLocal. Shows up as
   * leaks in unit tests where lead JVM size keeps on increasing with new tests.
   */
  private def suspendActiveSession[T](function: => T): T = {
    SparkSession.getActiveSession match {
      case Some(activeSession) =>
        SparkSession.clearActiveSession()
        try {
          function
        } finally {
          SparkSession.setActiveSession(activeSession)
        }
      case None => function
    }
  }

  override def close(): Unit = {}

  private[hive] def closeHive(clearCache: Boolean): Unit = synchronized {
    if (clearCache) invalidateAll()
    // Non-isolated client can be closed here directly which is only present in snappy-spark
    // using the new property HiveUtils.HIVE_METASTORE_ISOLATION not present in upstream.
    // Isolated loader would require reflection but that case is only in smart connector
    // which will never use embedded mode SnappyHiveExternalCatalog but we do need to check
    // for the case of local mode execution with upstream Spark.
    hiveClient match {
      case client: HiveClientImpl if !client.clientLoader.isolationOn =>
        val loader = client.clientLoader
        val hive = loader.cachedHive
        if (hive != null) {
          loader.cachedHive = null
          Hive.set(hive.asInstanceOf[Hive])
          Hive.closeCurrent()
        }
      case _ =>
    }
  }
}

object SnappyHiveExternalCatalog extends SparkSupport {

  @GuardedBy("this")
  private[this] var instance: SnappyHiveExternalCatalog = _

  /**
   * Hack for CTAS for builtin tables that need to pre-create the tables before
   * insert for the store layer to find them. This flag allows handling of this
   * case in the ExternalCatalog.createTable method.
   */
  private[sql] val ignoreIfExists: ThreadLocal[java.lang.Boolean] =
    new ThreadLocal[java.lang.Boolean]()

  @GuardedBy("this")
  private[hive] def getInstanceIfCurrent: Option[SnappyHiveExternalCatalog] = {
    val catalog = instance
    if (catalog ne null) {
      // Check if it is being invoked for the same instance of GemFireStore.
      // We don't store the store instance itself to avoid a dangling reference to
      // entire store even after shutdown, rather compare its creation time.
      if (Misc.getMemStoreBooting.getCreateTime == catalog.createTime) Some(catalog)
      else {
        close()
        None
      }
    } else None
  }

  def newInstance(sparkConf: SparkConf,
      hadoopConf: Configuration): SnappyHiveExternalCatalog = synchronized {
    // Reduce log level to error during hive client initialization
    // as it generates hundreds of lines of logs which are of no use.
    // Once the initialization is done, restore the logging level.
    val logger = Misc.getI18NLogWriter.asInstanceOf[GFToSlf4jBridge]
    val previousLevel = logger.getLevel
    val log4jLogger = LogManager.getRootLogger
    val log4jLevel = log4jLogger.getEffectiveLevel
    logger.info("Starting hive meta-store initialization")
    val reduceLog = previousLevel == LogWriterImpl.CONFIG_LEVEL ||
        previousLevel == LogWriterImpl.INFO_LEVEL
    if (reduceLog) {
      logger.setLevel(LogWriterImpl.ERROR_LEVEL)
      log4jLogger.setLevel(Level.ERROR)
    }
    try {
      instance = internals.newEmbeddedHiveCatalog(sparkConf, hadoopConf,
        Misc.getMemStoreBooting.getCreateTime)
    } finally {
      logger.setLevel(previousLevel)
      log4jLogger.setLevel(log4jLevel)
      logger.info("Done hive meta-store initialization")
    }
    instance
  }

  private[sql] def getExistingInstance: SnappyHiveExternalCatalog = synchronized {
    if (instance ne null) instance
    else throw new CacheClosedException("No external catalog instance available")
  }

  private[sql] def getInstance: SnappyHiveExternalCatalog = synchronized(instance)

  def close(): Unit = synchronized {
    if (instance ne null) {
      instance.withHiveExceptionHandling(instance.closeHive(clearCache = true))
      instance = null
    }
  }
}
