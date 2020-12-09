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
package io.snappydata.sql.catalog.impl

import java.sql.SQLException
import java.util.Collections
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.LocalRegion
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.sql.catalog.{ConnectorExternalCatalog, RelationInfo, SmartConnectorHelper, SnappyExternalCatalog}
import io.snappydata.thrift.{CatalogMetadataDetails, CatalogMetadataRequest, snappydataConstants}

import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, NoSuchPermanentFunctionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression}
import org.apache.spark.sql.collection.{SmartExecutorBucketPartition, Utils}
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyContext, SparkSession, TableNotFoundException, ThinClientConnectorMode}

/**
 * An ExternalCatalog implementation for the smart connector mode.
 *
 * Note that unlike other ExternalCatalog implementations, this is created one for each session
 * rather than being a singleton in the SharedState because each request needs to be authenticated
 * independently using the credentials of the user that created the session. Consequently calls
 * to "sharedState.externalCatalog()" will return null in smart connector mode and should never
 * be used. For internal code paths in Spark that use it, an alternative dummy global might
 * be added later that switches the user authentication using thread-locals or similar, but as
 * of now it is used only by some hive insert paths which are not used in SnappySessionState.
 */
abstract class SmartConnectorExternalCatalog extends SnappyExternalCatalog {

  val session: SparkSession

  def jdbcUrl: String = SnappyContext.getClusterMode(session.sparkContext)
      .asInstanceOf[ThinClientConnectorMode].url

  @GuardedBy("this")
  private[this] var _connectorHelper: SmartConnectorHelper = _

  @GuardedBy("this")
  private[this] def connectorHelper: SmartConnectorHelper = {
    val helper = _connectorHelper
    if (helper ne null) helper
    else {
      _connectorHelper = new SmartConnectorHelper(session, jdbcUrl)
      _connectorHelper
    }
  }

  protected[catalog] def helper: SmartConnectorHelper = connectorHelper

  protected[catalog] def withExceptionHandling[T](function: => T): T = synchronized {
    try {
      function
    } catch {
      case e: SQLException if isConnectionException(e) =>
        // attempt to create a new connection
        if (_connectorHelper ne null) _connectorHelper.close()
        _connectorHelper = new SmartConnectorHelper(session, jdbcUrl)
        function
    }
  }

  protected def isConnectionException(e: SQLException): Boolean = {
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
        e.getSQLState.startsWith(SQLState.LANG_DEAD_STATEMENT) ||
        e.getSQLState.startsWith(SQLState.GFXD_NODE_SHUTDOWN_PREFIX)
  }

  override def invalidate(name: (String, String)): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
  }

  override def invalidateCaches(relations: Seq[(String, String)]): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
    // there is no version update in this call here, rather only the caches are cleared
    RefreshMetadata.executeLocal(RefreshMetadata.UPDATE_CATALOG_SCHEMA_VERSION, args = null)
  }

  def invalidateAll(): Unit = {
    // invalidate all the RelationInfo objects inside as well as the cache itself
    val iter = ConnectorExternalCatalog.cachedCatalogTables.asMap().values().iterator()
    while (iter.hasNext) {
      iter.next()._2 match {
        case Some(info) => info.invalid = true
        case None =>
      }
    }
    ConnectorExternalCatalog.cachedCatalogTables.invalidateAll()
  }

  def close(): Unit = synchronized(connectorHelper.close())

  // Using a common procedure to update catalog meta-data for create/drop/alter methods
  // and likewise a common procedure to get catalog meta-data for get/exists/list methods

  protected def createDatabaseImpl(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogDatabase(ConnectorExternalCatalog.convertFromCatalogDatabase(dbDefinition))
    request.setExists(ignoreIfExists)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_DATABASE, request))
  }

  protected def dropDatabaseImpl(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames(Collections.singletonList(db)).setExists(ignoreIfNotExists)
        .setOtherFlags(Collections.singletonList(flag(cascade)))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_DATABASE, request))
  }

  override def getDatabase(db: String): CatalogDatabase = {
    if (db == SnappyExternalCatalog.SYS_DATABASE) return systemDatabaseDefinition
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_DATABASE, request))
    if (result.isSetCatalogDatabase) {
      ConnectorExternalCatalog.convertToCatalogDatabase(result.getCatalogDatabase)
    } else throw SnappyExternalCatalog.databaseNotFoundException(db)
  }

  override def databaseExists(db: String): Boolean = {
    // this is invoked frequently so instead of messaging right away, check the common
    // case if there exists a cached table in the database
    if (db == SnappyExternalCatalog.SYS_DATABASE) true
    else {
      val itr = ConnectorExternalCatalog.cachedCatalogTables.asMap().keySet().iterator()
      while (itr.hasNext) {
        val tableWithDatabase = itr.next()
        if (tableWithDatabase._1 == db) return true
      }
      val request = new CatalogMetadataRequest()
      request.setDatabaseName(db)
      withExceptionHandling(connectorHelper.getCatalogMetadata(
        snappydataConstants.CATALOG_DATABASE_EXISTS, request)).exists
    }
  }

  override def listDatabases(): Seq[String] = listDatabases("*")

  override def listDatabases(pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_DATABASES, request)).getNames.asScala
  }

  override def setCurrentDatabase(db: String): Unit = synchronized {
    connectorHelper.setCurrentDatabase(db)
  }

  protected def createTableImpl(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogTable(ConnectorExternalCatalog.convertFromCatalogTable(table))
        .setExists(ignoreIfExists)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  protected def dropTableImpl(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava).setExists(ignoreIfNotExists)
        .setOtherFlags(Collections.singletonList(flag(purge)))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  protected def alterTableImpl(table: CatalogTable): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogTable(ConnectorExternalCatalog.convertFromCatalogTable(table))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  protected def alterTableSchemaImpl(dbName: String, table: String,
      newSchema: StructType): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((dbName :: table :: Nil).asJava).setNewSchema(newSchema.json)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_TABLE_SCHEMA, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  protected def alterTableStatsImpl(db: String, table: String,
      stats: Option[(BigInt, Option[BigInt], Map[String, Any])]): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava)
    stats match {
      case None =>
      case Some(s) =>
        val catalogTable = getTable(db, table)
        request.setCatalogStats(ConnectorExternalCatalog.convertFromCatalogStatistics(
          catalogTable.schema, s._1, s._2, s._3))
    }
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_TABLE_STATS, request))

    invalidate(db -> table)
  }

  protected def renameTableImpl(db: String, oldName: String, newName: String): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: oldName :: newName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  override def createPolicy(dbName: String, policyName: String, targetTable: String,
      policyFor: String, policyApplyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      owner: String, filterString: String): Unit = {
    throw Utils.analysisException("CREATE POLICY for Row Level Security " +
        "not supported for smart connector mode")
  }

  override protected def getCachedCatalogTable(db: String, table: String): CatalogTable = {
    ConnectorExternalCatalog.getCatalogTable(db -> table, catalog = this)
  }

  override def getRelationInfo(db: String, table: String,
      rowTable: Boolean): (RelationInfo, Option[LocalRegion]) = {
    if (db == SnappyExternalCatalog.SYS_DATABASE) {
      // SYS tables are treated as single partition replicated tables visible
      // from all executors using the JDBC connection
      RelationInfo(1, isPartitioned = false, partitions = Array(
        new SmartExecutorBucketPartition(0, 0, ArrayBuffer.empty))) -> None
    } else {
      assert(db.length > 0)
      ConnectorExternalCatalog.getRelationInfo(db -> table, catalog = this) match {
        case None => throw new TableNotFoundException(db, s"RealtionInfo for $table")
        case Some(r) => r -> None
      }
    }
  }

  override def tableExists(db: String, table: String): Boolean = {
    if (ConnectorExternalCatalog.cachedCatalogTables.getIfPresent(db -> table) ne null) true
    else {
      val request = new CatalogMetadataRequest()
      request.setDatabaseName(db).setNameOrPattern(table)
      withExceptionHandling(connectorHelper.getCatalogMetadata(
        snappydataConstants.CATALOG_TABLE_EXISTS, request)).exists
    }
  }

  override def listTables(db: String): Seq[String] = listTables(db, "*")

  override def listTables(db: String, pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_TABLES, request)).getNames.asScala
  }

  private def flag(b: Boolean): java.lang.Integer = if (b) 1 else 0

  override def loadTable(db: String, table: String, loadPath: String,
      isOverwrite: Boolean, holdDDLTime: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: loadPath :: Nil).asJava)
        .setOtherFlags((flag(isOverwrite) :: flag(holdDDLTime) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_TABLE, request))

    invalidateCaches(db -> table :: Nil)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava).setCatalogPartitions(parts.map(
      ConnectorExternalCatalog.convertFromCatalogPartition).asJava).setExists(ignoreIfExists)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_PARTITIONS, request))

    invalidateCaches(db -> table :: Nil)
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava).setProperties(parts.map(_.asJava).asJava)
        .setExists(ignoreIfNotExists)
        .setOtherFlags((flag(purge) :: flag(retainData) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_PARTITIONS, request))

    invalidateCaches(db -> table :: Nil)
  }

  override def alterPartitions(db: String, table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava).setCatalogPartitions(parts.map(
      ConnectorExternalCatalog.convertFromCatalogPartition).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_PARTITIONS, request))

    invalidateCaches(db -> table :: Nil)
  }

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: Nil).asJava).setProperties(specs.map(_.asJava).asJava)
        .setNewProperties(newSpecs.map(_.asJava).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_PARTITIONS, request))

    invalidateCaches(db -> table :: Nil)
  }

  override def loadPartition(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, isOverwrite: Boolean, holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: loadPath :: Nil).asJava)
        .setProperties(Collections.singletonList(partition.asJava)).setOtherFlags(
      (flag(isOverwrite) :: flag(holdDDLTime) :: flag(inheritTableSpecs) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_PARTITION, request))

    invalidateCaches(db -> table :: Nil)
  }

  protected def loadDynamicPartitionsImpl(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((db :: table :: loadPath :: Nil).asJava)
        .setProperties(Collections.singletonList(partition.asJava)).setOtherFlags(
      (flag(replace) :: Int.box(numDP) :: flag(holdDDLTime) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_DYNAMIC_PARTITIONS, request))

    invalidateCaches(db -> table :: Nil)
  }

  override def getPartition(db: String, table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    getPartitionOption(db, table, spec) match {
      case Some(p) => p
      case None => throw new NoSuchPartitionException(db, table, spec)
    }
  }

  override def getPartitionOption(db: String, table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(table).setProperties(spec.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_PARTITION, request))
    if (result.getCatalogPartitionsSize == 1) {
      Some(ConnectorExternalCatalog.convertToCatalogPartition(result.getCatalogPartitions.get(0)))
    } else None
  }

  override def listPartitionNames(db: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(table)
    if (partialSpec.isDefined) request.setProperties(partialSpec.get.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_PARTITION_NAMES, request))
    result.getNames.asScala
  }

  override def listPartitions(db: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(table)
    if (partialSpec.isDefined) request.setProperties(partialSpec.get.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_PARTITIONS, request))
    if (result.getCatalogPartitionsSize > 0) {
      result.getCatalogPartitions.asScala.map(ConnectorExternalCatalog.convertToCatalogPartition)
    } else Nil
  }

  protected def listPartitionsByFilterImpl(db: String, table: String,
      predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    // taken from HiveExternalCatalog.listPartitionsByFilter
    val catalogTable = getTable(db, table)
    val partitionColumnNames = catalogTable.partitionColumnNames.toSet
    val nonPartitionPruningPredicates = predicates.filterNot {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (nonPartitionPruningPredicates.nonEmpty) {
      throw new IllegalArgumentException("Expected only partition pruning predicates: " +
          predicates.reduceLeft(And))
    }

    val partitionSchema = catalogTable.partitionSchema
    val partitions = listPartitions(db, table, None)
    if (predicates.nonEmpty) {
      val boundPredicate = predicates.reduce(And).transform {
        case attr: AttributeReference =>
          val index = partitionSchema.indexWhere(_.name == attr.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      }
      partitions.filter(p => boundPredicate.eval(internals.catalogTablePartitionToRow(
        p, partitionSchema, defaultTimeZoneId)).asInstanceOf[Boolean])
    } else partitions
  }

  protected def createFunctionImpl(db: String, function: CatalogFunction): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogFunction(ConnectorExternalCatalog.convertFromCatalogFunction(function))
        .setNames(Collections.singletonList(db))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_FUNCTION, request))
  }

  protected def dropFunctionImpl(db: String, funcName: String): Unit = {
    val request = new CatalogMetadataDetails().setNames((db :: funcName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_FUNCTION, request))
  }

  protected def alterFunctionImpl(db: String, function: CatalogFunction): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogFunction(ConnectorExternalCatalog.convertFromCatalogFunction(function))
        .setNames(Collections.singletonList(db))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_FUNCTION, request))
  }

  protected def renameFunctionImpl(db: String, oldName: String, newName: String): Unit = {
    val request = new CatalogMetadataDetails()
        .setNames((db :: oldName :: newName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_FUNCTION, request))
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(funcName)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_FUNCTION, request))
    if (result.isSetCatalogFunction) {
      ConnectorExternalCatalog.convertToCatalogFunction(result.getCatalogFunction)
    } else throw new NoSuchPermanentFunctionException(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(funcName)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_FUNCTION_EXISTS, request)).exists
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setDatabaseName(db).setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_FUNCTIONS, request)).getNames.asScala
  }
}
