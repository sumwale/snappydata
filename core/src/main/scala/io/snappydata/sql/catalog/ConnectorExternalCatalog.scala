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
package io.snappydata.sql.catalog

import java.util.Collections
import java.util.concurrent.Callable

import scala.collection.JavaConverters._

import com.google.common.cache.{Cache, CacheBuilder}
import io.snappydata.Property
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import io.snappydata.thrift._

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils.EMPTY_STRING_ARRAY
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSupport, TableNotFoundException}
import org.apache.spark.{Logging, Partition, SparkEnv}

object ConnectorExternalCatalog extends Logging with SparkSupport {

  def cacheSize: Int = {
    SparkEnv.get match {
      case null => Property.CatalogCacheSize.defaultValue.get
      case env => Property.CatalogCacheSize.get(env.conf)
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  // noinspection UnstableApiUsage
  private[catalog] lazy val cachedCatalogTables: Cache[(String, String),
      (CatalogTable, Option[RelationInfo])] = {
    CacheBuilder.newBuilder().maximumSize(cacheSize).build()
  }

  private def toArray(columns: java.util.List[String]): Array[String] =
    columns.toArray(new Array[String](columns.size()))

  private def convertToCatalogStorage(storage: CatalogStorage,
      storageProps: Map[String, String]): CatalogStorageFormat = {
    internals.newCatalogStorageFormat(Option(storage.getLocationUri),
      Option(storage.getInputFormat), Option(storage.getOutputFormat),
      Option(storage.getSerde), storage.compressed, storageProps)
  }

  private[snappydata] def convertToCatalogStatistics(schema: StructType, fullTableName: String,
      catalogStats: CatalogStats): AnyRef = {
    val colStats = schema.indices.flatMap { i =>
      val f = schema(i)
      val colStatsMap = catalogStats.colStats.get(i)
      if (colStatsMap.isEmpty) None
      else internals.columnStatFromMap(fullTableName, f, colStatsMap.asScala.toMap) match {
        case None => None
        case Some(s) => Some(f.name -> s)
      }
    }.toMap
    internals.toCatalogStatistics(BigInt(catalogStats.sizeInBytes),
      if (catalogStats.isSetRowCount) Some(BigInt(catalogStats.getRowCount)) else None, colStats)
  }

  private[snappydata] def convertToCatalogDatabase(
      catalogSchemaObject: CatalogSchemaObject): CatalogDatabase = {
    internals.newCatalogDatabase(catalogSchemaObject.getName, catalogSchemaObject.getDescription,
      catalogSchemaObject.getLocationUri, catalogSchemaObject.getProperties.asScala.toMap)
  }

  private[snappydata] def convertToCatalogTable(request: CatalogMetadataDetails,
      conf: => SQLConf): (CatalogTable, Option[RelationInfo]) = {
    val tableObj = request.getCatalogTable
    val identifier = TableIdentifier(tableObj.getTableName, Option(tableObj.getDatabaseName))
    val tableType = tableObj.getTableType match {
      case "EXTERNAL" => CatalogTableType.EXTERNAL
      case "MANAGED" => CatalogTableType.MANAGED
      case "VIEW" => CatalogTableType.VIEW
    }
    val tableProps = tableObj.getProperties.asScala.toMap
    val storage = tableObj.getStorage
    val storageProps = storage.properties.asScala.toMap
    val schema = ExternalStoreUtils.getTableSchema(tableObj.getTableSchema)
    // SnappyData tables have bucketOwners while hive managed tables have bucketColumns
    // The bucketSpec below is only for hive managed tables.
    val bucketSpec = if (tableObj.getBucketColumns.isEmpty) None
    else {
      Some(BucketSpec(tableObj.getNumBuckets, tableObj.getBucketColumns.asScala,
        tableObj.getSortColumns.asScala))
    }
    val stats = if (tableObj.isSetStats) {
      Some(convertToCatalogStatistics(schema, identifier.unquotedString, tableObj.getStats))
    } else None
    val bucketOwners = tableObj.getBucketOwners
    // remove partitioning columns from CatalogTable for row/column tables
    val partitionCols = if (bucketOwners.isEmpty) Utils.EMPTY_STRING_ARRAY
    else {
      val cols = tableObj.getPartitionColumns
      tableObj.setPartitionColumns(Collections.emptyList())
      toArray(cols)
    }
    val ignoredProps = if (tableObj.isSetIgnoredProperties) {
      tableObj.ignoredProperties.asScala.toMap
    } else Map.empty[String, String]
    val table = internals.newCatalogTable(identifier, tableType, ConnectorExternalCatalog
        .convertToCatalogStorage(storage, storageProps), schema, Option(tableObj.getProvider),
      tableObj.getPartitionColumns.asScala, bucketSpec, tableObj.getOwner, tableObj.createTime,
      tableObj.lastAccessTime, tableProps, stats, Option(tableObj.getViewOriginalText),
      Option(tableObj.getViewText), Option(tableObj.getComment),
      tableObj.getUnsupportedFeatures.asScala, tableObj.tracksPartitionsInCatalog,
      tableObj.schemaPreservesCase, ignoredProps)

    // if catalog schema version is not set then it indicates that RelationInfo was not filled
    // in due to region being destroyed or similar exception
    if (!request.isSetCatalogSchemaVersion) {
      return table -> None
    }
    val catalogSchemaVersion = request.getCatalogSchemaVersion
    if (bucketOwners.isEmpty) {
      // external tables (with source as csv, parquet etc.)
      table -> Some(RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY, EMPTY_STRING_ARRAY,
        EMPTY_STRING_ARRAY, Array.empty[Partition], catalogSchemaVersion))
    } else {
      val bucketCount = tableObj.getNumBuckets
      val indexCols = toArray(tableObj.getIndexColumns)
      val pkCols = toArray(tableObj.getPrimaryKeyColumns)
      if (bucketCount > 0) {
        val allNetUrls = SmartConnectorHelper.setBucketToServerMappingInfo(
          bucketCount, bucketOwners, conf)
        val partitions = SmartConnectorHelper.getPartitions(allNetUrls)
        table -> Some(RelationInfo(bucketCount, isPartitioned = true, partitionCols,
          indexCols, pkCols, partitions, catalogSchemaVersion))
      } else {
        val allNetUrls = SmartConnectorHelper.setReplicasToServerMappingInfo(
          tableObj.getBucketOwners.get(0).getSecondaries)
        val partitions = SmartConnectorHelper.getPartitions(allNetUrls)
        table -> Some(RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY, indexCols,
          pkCols, partitions, catalogSchemaVersion))
      }
    }
  }

  private[snappydata] def convertToCatalogPartition(
      partitionObj: CatalogPartitionObject): CatalogTablePartition = {
    val storage = partitionObj.getStorage
    CatalogTablePartition(partitionObj.getSpec.asScala.toMap,
      convertToCatalogStorage(storage, storage.getProperties.asScala.toMap))
  }

  /**
   * Convention in thrift CatalogFunctionObject.resources is list of "resourceType:uri" strings.
   */
  private def functionResource(fullName: String): FunctionResource = {
    val sepIndex = fullName.indexOf(':')
    val resourceType = FunctionResourceType.fromString(fullName.substring(0, sepIndex))
    FunctionResource(resourceType, fullName.substring(sepIndex + 1))
  }

  private[snappydata] def convertToCatalogFunction(
      functionObj: CatalogFunctionObject): CatalogFunction = {
    CatalogFunction(FunctionIdentifier(functionObj.getFunctionName,
      Option(functionObj.getDatabaseName)), functionObj.getClassName,
      functionObj.getResources.asScala.map(functionResource))
  }

  private def convertFromCatalogStorage(storage: CatalogStorageFormat): CatalogStorage = {
    val storageObj = new CatalogStorage(storage.properties.asJava, storage.compressed)
    val locationUri = internals.catalogStorageFormatLocationUri(storage)
    if (locationUri.isDefined) storageObj.setLocationUri(locationUri.get)
    if (storage.inputFormat.isDefined) storageObj.setInputFormat(storage.inputFormat.get)
    if (storage.outputFormat.isDefined) storageObj.setOutputFormat(storage.outputFormat.get)
    if (storage.serde.isDefined) storageObj.setSerde(storage.serde.get)
    storageObj
  }

  private def getOrNull(option: Option[String]): String = option match {
    case None => null
    case Some(v) => v
  }

  private[snappydata] def convertFromCatalogStatistics(schema: StructType, sizeInBytes: BigInt,
      rowCount: Option[BigInt], stats: Map[String, Any]): CatalogStats = {
    val colStats = schema.map { f =>
      stats.get(f.name) match {
        case None => Collections.emptyMap[String, String]()
        case Some(stat) => internals.columnStatToMap(stat, f.name, f.dataType).asJava
      }
    }.asJava
    val catalogStats = new CatalogStats(sizeInBytes.longValue(), colStats)
    rowCount match {
      case None => catalogStats
      case Some(c) => catalogStats.setRowCount(c.longValue())
    }
  }

  private[snappydata] def convertFromCatalogDatabase(
      catalogDatabase: CatalogDatabase): CatalogSchemaObject = {
    new CatalogSchemaObject(catalogDatabase.name, catalogDatabase.description,
      internals.catalogDatabaseLocationURI(catalogDatabase), catalogDatabase.properties.asJava)
  }

  private[snappydata] def convertFromCatalogTable(table: CatalogTable): CatalogTableObject = {
    val storageObj = convertFromCatalogStorage(table.storage)
    // non CatalogTable attributes like indexColumns, buckets will be set by caller
    // in the GET_CATALOG_DATABASE system procedure hence filled as empty here
    val (numBuckets, bucketColumns, sortColumns) = table.bucketSpec match {
      case None => (-1, Collections.emptyList[String](), Collections.emptyList[String]())
      case Some(spec) => (spec.numBuckets, spec.bucketColumnNames.asJava,
          spec.sortColumnNames.asJava)
    }
    val tableObj = new CatalogTableObject(table.identifier.table, table.tableType.name,
      storageObj, table.schema.json, table.partitionColumnNames.asJava, Collections.emptyList(),
      Collections.emptyList(), Collections.emptyList(), bucketColumns, sortColumns,
      table.owner, table.createTime, table.lastAccessTime, table.properties.asJava,
      table.unsupportedFeatures.asJava, table.tracksPartitionsInCatalog,
      table.schemaPreservesCase)
    tableObj.setDatabaseName(getOrNull(table.identifier.database))
        .setProvider(getOrNull(table.provider))
        .setViewText(getOrNull(table.viewText))
        .setViewOriginalText(getOrNull(internals.catalogTableViewOriginalText(table)))
        .setComment(getOrNull(table.comment))
    val ignoredProps = internals.catalogTableIgnoredProperties(table)
    if (ignoredProps.nonEmpty) tableObj.setIgnoredProperties(ignoredProps.asJava)
    if (numBuckets != -1) tableObj.setNumBuckets(numBuckets)
    table.stats match {
      case None => tableObj
      case Some(stats) => tableObj.setStats(convertFromCatalogStatistics(table.schema,
        stats.sizeInBytes, stats.rowCount, stats.colStats))
    }
  }

  private[snappydata] def convertFromCatalogPartition(
      partition: CatalogTablePartition): CatalogPartitionObject = {
    new CatalogPartitionObject(partition.spec.asJava,
      convertFromCatalogStorage(partition.storage), partition.parameters.asJava)
  }

  private[snappydata] def convertFromCatalogFunction(
      function: CatalogFunction): CatalogFunctionObject = {
    val resources = function.resources.map(r => s"${r.resourceType.resourceType}:${r.uri}")
    new CatalogFunctionObject(function.identifier.funcName,
      function.className, resources.asJava).setDatabaseName(getOrNull(
      function.identifier.database))
  }

  private def loadFromCache(name: (String, String),
      catalog: SmartConnectorExternalCatalog): (CatalogTable, Option[RelationInfo]) = {
    // avoid callable object creation if possible
    cachedCatalogTables.getIfPresent(name) match {
      case null =>
        cachedCatalogTables.get(name, new Callable[(CatalogTable, Option[RelationInfo])] {
          override def call(): (CatalogTable, Option[RelationInfo]) = {
            logDebug(s"Looking up data source for $name")
            val request = new CatalogMetadataRequest()
            request.setDatabaseName(name._1).setNameOrPattern(name._2)
            val result = catalog.withExceptionHandling(catalog.helper.getCatalogMetadata(
              snappydataConstants.CATALOG_GET_TABLE, request))
            if (!result.isSetCatalogTable) throw new TableNotFoundException(name._1, name._2)
            convertToCatalogTable(result, catalog.session.sessionState.conf)
          }
        })
      case result => result
    }
  }

  def getCatalogTable(name: (String, String),
      catalog: SmartConnectorExternalCatalog): CatalogTable = {
    loadFromCache(name, catalog)._1
  }

  def getRelationInfo(name: (String, String),
      catalog: SmartConnectorExternalCatalog): Option[RelationInfo] = {
    loadFromCache(name, catalog)._2
  }

  def close(): Unit = synchronized {
    cachedCatalogTables.invalidateAll()
  }
}

case class RelationInfo(numBuckets: Int,
    isPartitioned: Boolean,
    partitioningCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    indexCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    pkCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    partitions: Array[org.apache.spark.Partition] = Array.empty,
    catalogSchemaVersion: Long = -1) {

  @transient
  @volatile var invalid: Boolean = _
}
