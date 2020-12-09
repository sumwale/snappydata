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

package io.snappydata.recovery

import java.sql.ResultSet
import java.util.function.BiConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Random, Try}

import com.gemstone.gemfire.distributed.internal.DistributionManager
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper
import com.gemstone.gemfire.internal.shared.SystemProperties
import com.gemstone.gnu.trove.{TLinkableAdaptor, TLinkedList}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.engine.distributed.RecoveryModeResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode
import com.pivotal.gemfirexd.internal.engine.distributed.message.PersistentStateInRecoveryMode.{RecoveryModePersistentView, RecoveryModePersistentViewPair}
import com.pivotal.gemfirexd.internal.engine.sql.execute.RecoveredMetadataRequestMessage
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyIndexStats, SnappyRegionStats}
import com.pivotal.gemfirexd.{Attribute, Constants}
import io.snappydata.sql.catalog.{CatalogObjectType, ConnectorExternalCatalog, SnappyExternalCatalog}
import io.snappydata.thrift.{CatalogFunctionObject, CatalogMetadataDetails, CatalogPartitionObject, CatalogSchemaObject, CatalogTableObject}
import io.snappydata.{Constant, PermissionChecker}
import org.apache.hadoop.fs.Path
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.parboiled2.{Rule0, Rule1}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils.{toLowerCase => lower, toUpperCase => upper}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.{CaseInsensitiveMutableHashMap, escapeSingleQuotedString}
import org.apache.spark.sql.execution.command.{DDLUtils, ShowCreateTableCommand}
import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.sql.internal.{ContextJarUtils, SQLConf}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.JdbcExtendedUtils.{quotedName => quoteTableName}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{ParseException, SnappyContext, SnappyParser, SnappyParserConsts, SparkSupport}

object RecoveryService extends SparkSupport with Logging {

  private var recoveryStats: (
      Seq[SnappyRegionStats], Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = _
  private var catalogTableCount = 0
  private var enableTableCountInUI: Boolean = _

  private lazy val recoveryCatalog = HiveClientUtil.getOrCreateExternalCatalog(
    SnappyContext.globalSparkContext, () => SnappyContext.globalSparkContext.getConf)

  private lazy val session = {
    val session = SnappyContext().snappySession
    if (Misc.isSecurityEnabled) {
      session.conf.set(Attribute.USERNAME_ATTR,
        Misc.getMemStore.getBootProperty(Attribute.USERNAME_ATTR))
      session.conf.set(Attribute.PASSWORD_ATTR,
        Misc.getMemStore.getBootProperty(Attribute.PASSWORD_ATTR))
    }
    session
  }

  private def isColumnTable(table: CatalogTable): Boolean =
    CatalogObjectType.isColumnTable(CatalogObjectType.getTableType(table))

  private def isStoreManagedTable(table: CatalogTable): Boolean =
    CatalogObjectType.isTableBackedByRegion(CatalogObjectType.getTableType(table))

  def getStats: (Seq[SnappyRegionStats], Seq[SnappyIndexStats],
      Seq[SnappyExternalTableStats]) = synchronized {
    if (recoveryStats == null || catalogTableCount != allRecoveredTables.size) {
      val allTables = allRecoveredTables
      var tblCounts: Seq[SnappyRegionStats] = Nil
      catalogTableCount = allTables.length
      allTables.foreach(table => {
        try {
          table.storage.locationUri match {
            case Some(_) => // external tables can be seen in show tables but filtered out in UI
            case None =>
              if (recoveryStats == null ||
                  !recoveryStats._1.exists(_.getTableName.equalsIgnoreCase(table.qualifiedName))) {
                logDebug(s"Querying table: $table for count")
              }
              val recCount = if (enableTableCountInUI) {
                session.sql(s"SELECT count(1) FROM ${table.identifier.quotedString}")
                    .collect()(0).getLong(0)
              } else -1L
              val (numBuckets, isReplicatedTable) = RecoveryService
                  .getNumBuckets(upper(table.database), upper(table.identifier.table))
              val regionStats = new SnappyRegionStats()
              regionStats.setRowCount(recCount)
              regionStats.setTableName(table.qualifiedName)
              regionStats.setReplicatedTable(isReplicatedTable)
              regionStats.setBucketCount(numBuckets)
              regionStats.setColumnTable(isColumnTable(table))
              tblCounts :+= regionStats
          }
        } catch {
          case e: Exception => logError(s"Error querying table $table.\n$e")
            val regionStats = new SnappyRegionStats()
            regionStats.setRowCount(-1L)
            regionStats.setTableName(table.qualifiedName)
            regionStats.setColumnTable(isColumnTable(table))
            tblCounts :+= regionStats
        }
      })
      recoveryStats = (tblCounts, Nil, Nil)
    }
    recoveryStats
  }

  def getAllDDLs: Seq[String] = {
    val ddlsPart1 = new mutable.ArrayBuffer[String]
    val ddlsPart2 = new mutable.ArrayBuffer[String]

    val dbList = recoveryCatalog.listDatabases().filter(dbName =>
      !dbName.equalsIgnoreCase(SnappyExternalCatalog.SYS_DATABASE))
    var allDatabases = dbList.map(dbName => lower(dbName) ->
        recoveryCatalog.getDatabase(dbName)).toMap
    val authKey = SnappyParserConsts.AUTHORIZATION.lower

    def addToBuffer(ddl: String): Unit = {
      val recoveryParser = SnappyRecoveryParser
      if (recoveryParser.isStoreDDL(ddl)) {
        // put the 'initialization' DDLs to be in a separate buffer that will be at the start
        if (recoveryParser.isStoreInitDDL(ddl)) {
          // check if the previous DDL was a USE DATABASE, in which case add to ddlInitBuffer too
          val len = ddlsPart2.length
          if (len > 0 && recoveryParser.isUseDatabase(ddlsPart2(len - 1))) {
            ddlsPart1 += ddlsPart2(len - 1)
          }
          ddlsPart1 += ddl
        } else ddlsPart2 += ddl
      } else recoveryParser.getCreateDatabaseWithAuth(ddl) match {
        case Some(dbName) =>
          // check if metastore entry is old-style without 'authorization' property
          val normalizedDb = lower(dbName)
          allDatabases.get(normalizedDb) match {
            case Some(db) =>
              if (!db.properties.contains(authKey)) {
                ddlsPart1 += ddl
                allDatabases -= normalizedDb
              }
            case _ => ddlsPart1 += ddl
          }
        case _ =>
      }
    }

    if (Misc.getGemFireCache.isSnappyRecoveryMode) {
      mostRecentMemberObject.getOtherDDLs.asScala.foreach(addToBuffer)
    } else {
      val rsResult = new Array[ResultSet](1)
      GfxdSystemProcedures.EXPORT_ALL_DDLS(Boolean.box(true), rsResult)
      val rs = rsResult(0)
      while (rs.next()) {
        if (!Misc.SNAPPY_HIVE_METASTORE.equalsIgnoreCase(rs.getString(1)) &&
            !Misc.SNAPPY_HIVE_METASTORE.equalsIgnoreCase(rs.getString(2))) {
          addToBuffer(rs.getString(3))
        }
      }
    }

    // add CREATE DATABASE statements
    dbList.foreach(dbName => allDatabases.get(lower(dbName)) match {
      case Some(db) =>
        val commentStr =
          if (db.description.isEmpty) ""
          else s"\nCOMMENT '${escapeSingleQuotedString(db.description)}'"
        val authStr = Utils.getDatabaseAuthorizationClause(db.properties)
        val dbProps = if (authStr.isEmpty) db.properties else db.properties - authKey
        val locationStr = escapeSingleQuotedString(new Path(db.locationUri).toString)
        val propsStr =
          if (dbProps.isEmpty) ""
          else {
            dbProps.map(kv => s"${quoteIdentifier(kv._1)} '${escapeSingleQuotedString(kv._2)}'")
                .mkString(s"WITH DBPROPERTIES (\n  ", ",\n  ", "\n)")
          }
        ddlsPart1 +=
            s"""
               |CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(dbName)}$commentStr
               |LOCATION '$locationStr'$propsStr$authStr
               |""".stripMargin
      case _ =>
    })

    val ldapPrefix = Constants.LDAP_GROUP_PREFIX
    val packageConsumer = new BiConsumer[String, Object] {
      def accept(alias: String, cmdObj: Object): Unit = cmdObj match {
        case cmd: String if alias != Constant.CLUSTER_ID &&
            alias != Constant.INTP_GRANT_REVOKE_KEY &&
            !alias.startsWith(Constant.EXTERNAL_TABLE_REGION_KEY_PREFIX) &&
            !alias.startsWith(Constant.MEMBER_ID_PREFIX) &&
            !alias.startsWith(ContextJarUtils.functionKeyPrefix) &&
            alias != ContextJarUtils.droppedFunctionsKey =>
          val cmdFields = cmd.split("\\|", -1)
          val urlStr = escapeSingleQuotedString(cmdFields(0))
          if (cmdFields.length > 1) {
            val repos = cmdFields(1)
            val repoStr =
              if (repos.isEmpty) "" else s"\nREPOS '${escapeSingleQuotedString(repos)}'"
            val path = cmdFields(2)
            val pathStr = if (path.isEmpty) "" else s"\nPATH '${escapeSingleQuotedString(path)}'"
            ddlsPart1 += s"DEPLOY PACKAGE $alias '$urlStr'$repoStr$pathStr"
          } else {
            ddlsPart1 += s"DEPLOY JAR $alias '$urlStr'"
          }
        case perms: PermissionChecker =>
          if (alias == Constant.INTP_GRANT_REVOKE_KEY) {
            // GRANT for scala interpreter
            val allowedUsers = perms.getAllowedUsers
            if (allowedUsers.length > 0) {
              for (user <- allowedUsers) {
                ddlsPart2 += s"GRANT PRIVILEGE EXEC SCALA TO ${quoteIdentifier(user)}"
              }
            }
            val allowedGroups = perms.getAllowedGroups
            if (allowedGroups.length > 0) {
              for (group <- allowedGroups) {
                ddlsPart2 += s"GRANT PRIVILEGE EXEC SCALA TO $ldapPrefix${quoteIdentifier(group)}"
              }
            }
          } else if (alias.startsWith(Constant.EXTERNAL_TABLE_REGION_KEY_PREFIX)) {
            // GRANT on external table
            val quotedTable = quoteTableName(
              alias.substring(Constant.EXTERNAL_TABLE_REGION_KEY_PREFIX.length))
            val allowedUsers = perms.getAllowedUsers
            if (allowedUsers.length > 0) {
              for (user <- allowedUsers) {
                ddlsPart2 += s"GRANT ALL ON $quotedTable TO ${quoteIdentifier(user)}"
              }
            }
            val allowedGroups = perms.getAllowedGroups
            if (allowedGroups.length > 0) {
              for (group <- allowedGroups) {
                ddlsPart2 += s"GRANT ALL ON $quotedTable TO $ldapPrefix${quoteIdentifier(group)}"
              }
            }
          } else {
            logError(s"Unknown key for PermissionChecker in metadata commands region: $alias")
          }
        case _ =>
          logError(s"Unknown key/value in metadata commands region: [$alias] = [$cmdObj]")
      }
    }
    Misc.getMemStore.getMetadataCmdRgn.forEach(packageConsumer)

    // add CREATE FUNCTION statements to ddlsPart2 so that any CREATE VIEWs added
    // are after the function definitions (those can potentially use UDAFs)
    dbList.foreach(dbName => recoveryCatalog.listFunctions(dbName, "*").foreach { fName =>
      val func = recoveryCatalog.getFunction(dbName, fName)
      val (funcClass, funcRetType) = func.className.lastIndexOf("__") match {
        case -1 => func.className -> ""
        case index => func.className.substring(0, index) -> func.className.substring(index + 2)
      }
      val returns = if (funcRetType.isEmpty) "" else " RETURNS " + funcRetType
      val resources = func.resources.map(r =>
        s"${r.resourceType.resourceType} '${escapeSingleQuotedString(r.uri)}'")
          .mkString("\nUSING ", ",\n  ", "")
      ddlsPart2 +=
          s"""
             |CREATE FUNCTION ${func.identifier.quotedString}
             |AS '${escapeSingleQuotedString(funcClass)}'$returns$resources
             |""".stripMargin
    })

    // add CREATE TABLE statements ordered by timestamp and also ensuring that dependencies
    // always are after the base table (in case timestamps are incorrect)
    val currentTime = System.currentTimeMillis()
    val allTables = new java.util.ArrayList[CatalogTableNode]()
    dbList.foreach(db => recoveryCatalog.listTables(db).foreach(table =>
      recoveryCatalog.getTableIfExists(db, table) match {
        case Some(catalogTable) => allTables.add(new CatalogTableNode(catalogTable, currentTime))
        case _ =>
      }))

    allTables.sort(Ordering.fromLessThan[CatalogTableNode](_.timestamp < _.timestamp))
    val numTables = allTables.size()
    // now create a double linked list and reorder dependencies to be always after the base table
    val tableList = new TLinkedList
    val tableMap = new UnifiedMap[String, CatalogTableNode](numTables)
    var index = 0
    while (index < numTables) {
      val node = allTables.get(index)
      val table = node.table
      logDebug(s"RecoveryService: getAllDDLs: table: $table")
      tableList.add(node)
      if (index != 0) {
        // ensure that timestamps are strictly ascending so that baseTable timestamp comparison
        // can check with certainty whether dependency is before or after it in the list
        val previous = node.getPrevious.asInstanceOf[CatalogTableNode]
        if (node.timestamp <= previous.timestamp) node.timestamp = previous.timestamp + 1L
      }
      tableMap.put(table.identifier.unquotedString, node)
      index += 1
    }
    var tableIter = tableList.iterator()
    while (tableIter.hasNext) {
      val current = tableIter.next().asInstanceOf[CatalogTableNode]
      // skip if already moved
      if (!current.reordered) {
        recoveryCatalog.getBaseTable(current.table) match {
          case Some(baseTable) => tableMap.get(baseTable) match {
            case baseNode if (baseNode ne null) && baseNode.timestamp > current.timestamp =>
              tableIter.remove()
              tableList.addBefore(baseNode.getNext, current)
              current.reordered = true
              // not strictly necessary due to reordered flag above, but done to keep list clean
              current.timestamp = baseNode.timestamp
            case _ =>
          }
          case _ =>
        }
      }
    }

    // add CREATE TABLE's in the final order
    // CREATE POLICY/INDEX require special handling while remaining table types work fine with
    // Spark's ShowCreateTable command

    // first ensure that any tables without any provider are treated as hive tables since that
    // is how Spark's ShowCreateTable spits out the DDLs
    ddlsPart1 += s"SET ${SQLConf.DEFAULT_DATA_SOURCE_NAME} = ${DDLUtils.HIVE_PROVIDER}"

    tableIter = tableList.iterator()
    while (tableIter.hasNext) {
      val table = tableIter.next().asInstanceOf[CatalogTableNode].table
      CatalogObjectType.getTableType(table) match {
        case CatalogObjectType.Policy =>
          val tableProperties = table.properties
          val targetTable = tableProperties(PolicyProperties.targetTable)
          val policyFor = tableProperties(PolicyProperties.policyFor)
          val policyApplyTo = tableProperties(PolicyProperties.policyApplyTo)
          val filter = tableProperties(PolicyProperties.filterString)
          ddlsPart1 +=
              s"""
                 |CREATE POLICY ${table.identifier.quotedString} ON ${quoteTableName(targetTable)}
                 |FOR $policyFor TO $policyApplyTo
                 |USING $filter
                 |""".stripMargin

        case CatalogObjectType.Index =>
          val storageProperties = new CaseInsensitiveMutableHashMap(table.storage.properties)
          val indexType = storageProperties.remove(ExternalStoreUtils.INDEX_TYPE) match {
            case Some("global hash") => "GLOBAL HASH "
            case Some("unique") => "UNIQUE "
            case _ => ""
          }
          val indexCols = storageProperties.remove(StoreUtils.PARTITION_BY).get
          val baseTable = quoteTableName(recoveryCatalog.getBaseTable(table).get)
          // remove other common options
          Array("serialization.format", "ALLOWEXISTING", SnappyExternalCatalog.DBTABLE_PROPERTY,
            ExternalStoreUtils.INDEX_NAME, SnappyExternalCatalog.INDEXED_TABLE,
            SnappyExternalCatalog.BASETABLE_PROPERTY).foreach(storageProperties.remove)
          val optionsStr =
            if (storageProperties.isEmpty) ""
            else {
              storageProperties.map(kv =>
                s"${quoteIdentifier(kv._1)} '${escapeSingleQuotedString(kv._2)}'")
                  .mkString("\nOPTIONS (\n  ", ",\n  ", "\n)")
            }
          ddlsPart1 +=
              s"""
                 |CREATE ${indexType}INDEX ${table.identifier.quotedString} ON $baseTable
                 |($indexCols)$optionsStr
                 |""".stripMargin

        case CatalogObjectType.View =>
          // add VIEWs to ddlsParts2 since their definitions could use UDAFs
          ddlsPart2 += ShowCreateTableCommand(table.identifier).run(session).head.getString(0)

        case _ =>
          ddlsPart1 += ShowCreateTableCommand(table.identifier).run(session).head.getString(0)
          // also add any ALTER ... PARTITION for the table
          if (table.partitionColumnNames.nonEmpty) try {
            val partitions = recoveryCatalog.listPartitions(table.database, table.identifier.table)
            if (partitions.nonEmpty) {
              val parts = partitions.map { part =>
                val locationUri = part.storage.locationUri
                val partitionEnd =
                  if (locationUri.isEmpty || locationUri == table.storage.locationUri) "\n)"
                  else {
                    val location = new Path(locationUri.get).toString
                    s"\n) LOCATION '${escapeSingleQuotedString(location)}'"
                  }
                part.parameters.map(kv =>
                  s"${quoteIdentifier(kv._1)} = '${escapeSingleQuotedString(kv._2)}'")
                    .mkString("PARTITION (\n  ", ",\n  ", partitionEnd)
              }.mkString("\n")
              ddlsPart2 +=
                  s"""
                     |ALTER TABLE ${table.identifier.quotedString} ADD IF NOT EXISTS
                     |$parts
                     |""".stripMargin
            }
          } catch {
            case _: Exception => // ignore
          }
      }
    }

    ddlsPart1 ++ ddlsPart2
  }

  private def getColRegionPath(dbName: String, tableName: String, bucketNumber: Int): String = {
    val internalFQTN = dbName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
    val regionPath = Misc.getRegionPath(internalFQTN)

    if (regionPath eq null) {
      throw new IllegalStateException(s"regionPath for $internalFQTN not found")
    } else {
      s"/_PR//B_${regionPath.substring(1, regionPath.length - 1)}/_$bucketNumber"
    }
  }

  private def getRowRegionPath(dbName: String, tableName: String, bucketId: Int): String = {
    val replicated = isReplicated(dbName, tableName)
    // using row region path as column regions will be colocated on same host
    val tablePath = "/" + dbName + "/" + tableName
    var bucketPath = tablePath
    if (!replicated) {
      val bucketName = PartitionedRegionHelper.getBucketName(tablePath, bucketId)
      bucketPath = s"/${PartitionedRegionHelper.PR_ROOT_REGION_NAME}/$bucketName"
    }
    bucketPath
  }

  /* table name and bucket number for PR r Column table, -1 indicates replicated row table */
  def getExecutorHost(dbName: String, tableName: String, bucketId: Int = -1): Seq[String] = {
    val rowRegionPath = getRowRegionPath(dbName, tableName, bucketId)
    // for null region maps select random host
    val rowRegionPathKey = combinedViewsMapSortedSet.keySet.filter(_.contains(rowRegionPath + "#$"))

    if (rowRegionPathKey.nonEmpty) {
      assert(rowRegionPathKey.size == 1,
        s"combinedViewsMapSortedSet should not contain multiple entries for the key $rowRegionPath")
      val viewPair = combinedViewsMapSortedSet(rowRegionPathKey.head).lastKey
      val hostCanonical = viewPair.getRowView.getExecutorHost
      val host = hostCanonical.split('(').head
      val executorHost = s"executor_${host}_$hostCanonical"
      logDebug(s"Host chosen for bucket: ${rowRegionPathKey.head}: $executorHost")
      executorHost :: Nil
    } else {
      logWarning(s"Preferred host is not found for bucket id: $bucketId. " +
          s"Choosing random host for $rowRegionPath")
      getRandomExecutorHost
    }
  }

  private def getRandomExecutorHost: Seq[String] = {
    val randomPersistentView = Random.shuffle(regionViewMapSortedSet.toList).head._2.firstKey
    val hostCanonical = randomPersistentView.getExecutorHost
    val host = hostCanonical.split('(').head
    s"executor_${host}_$hostCanonical" :: Nil
  }

  private val regionViewMapSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentView]] = mutable.Map.empty

  private val combinedViewsMapSortedSet: mutable.Map[String,
      mutable.SortedSet[RecoveryModePersistentViewPair]] = mutable.Map.empty

  private var locatorMember: InternalDistributedMember = _
  private val persistentObjectMemberMap: mutable.Map[
      InternalDistributedMember, PersistentStateInRecoveryMode] = mutable.Map.empty
  private var mostRecentMemberObject: PersistentStateInRecoveryMode = _
  private var memberObject: PersistentStateInRecoveryMode = _
  val schemaStructMap: mutable.Map[String, StructType] = mutable.Map.empty
  val versionMap: mutable.Map[String, Int] = collection.mutable.Map.empty
  val tableColumnIds: mutable.Map[String, Array[Int]] = mutable.Map.empty

  private def updateMetadataMaps(fqtnKey: String, alteredSchema: StructType,
      versionCnt: Int): Int = {
    schemaStructMap.put(s"$versionCnt#$fqtnKey", alteredSchema)
    val idArray: Array[Int] = new Array[Int](alteredSchema.fields.length)
    val prevSchema = schemaStructMap.getOrElse(s"${versionCnt - 1}#$fqtnKey", null)
    val prevIdArray: Array[Int] =
      tableColumnIds.getOrElse(s"${versionCnt - 1}#$fqtnKey", null)
    assert(prevSchema != null && prevIdArray != null)
    for (i <- alteredSchema.fields.indices) {
      val prevFieldIndex = prevSchema.fields.indexOf(alteredSchema.fields(i))
      idArray(i) = if (prevFieldIndex == -1) {
        // Alter Add column case
        idArray(i - 1) + 1
      } else {
        // Common column to previous schema
        prevIdArray(prevFieldIndex)
      }
    }
    // idArray contains column ids from alter statement
    tableColumnIds.put(s"$versionCnt#$fqtnKey", idArray)
    versionMap.put(fqtnKey, versionCnt)
    versionCnt + 1
  }

  private def createSchemasMap(): Unit = {
    val colParser = SnappyRecoveryParser
    allRecoveredTables.foreach(table => {
      // Create statements
      var versionCnt = 1
      val numOfSchemaParts = table.properties.getOrElse("NoOfschemaParts", "0").toInt
      assert(numOfSchemaParts != 0, "Schema string not found.")
      val schemaJsonStr = (0 until numOfSchemaParts)
          .map { i => table.properties.getOrElse(s"schemaPart.$i", null) }.mkString
      logDebug(s"RecoveryService: createSchemaMap: Schema Json String : $schemaJsonStr")
      val fqtnKey = table.identifier.database match {
        case Some(schName) => schName + "_" + table.identifier.table
        case None => throw new Exception(
          s"Schema name not found for the table ${table.identifier.table}")
      }
      var schema: StructType = StructType(DataType.fromJson(schemaJsonStr).asInstanceOf[StructType]
          .map(f => f.copy(name = lower(f.name))))

      assert(schema != null, s"schemaJson read from catalog table is null " +
          s"for ${table.identifier.table}")
      schemaStructMap.put(s"$versionCnt#$fqtnKey", schema)
      // for a table created with schema c1, c2 then c1 is dropped and then c1 is added
      // the added c1 is a new column and we want to be able to differentiate between both c1
      // so we assign ids to columns and new ids to columns from alter commands
      // tableColumnIds = Map("1#fqtn" -> Array(0, 1)). Entry "2#fqtn" -> Array(1, 2)
      // will be added when alter/add are identified for this table
      tableColumnIds.put(s"$versionCnt#$fqtnKey", schema.fields.indices.toArray)
      versionMap.put(fqtnKey, versionCnt)
      versionCnt += 1

      // Alter statements contains original sql with timestamp in keys to find sequence.for example
      // Properties: [k1=v1, altTxt_1568129777251=<alter command>, k3=v3]
      val altStmtKeysSorted = table.properties.keys
          .filter(_.contains(s"altTxt_")).toSeq
          .sortBy(_.split("_")(1).toLong)
      altStmtKeysSorted.foreach(k => {
        val stmt = upper(table.properties(k))
        var alteredSchema: StructType = null
        if (stmt.contains(" ADD COLUMN ")) {
          val columnString = stmt.substring(stmt.indexOf("ADD COLUMN ") + 11)
          val colNameAndType = (columnString.split("[ ]+")(0), columnString.split("[ ]+")(1))
          // along with not null other column definitions should be handled
          val colString = if (lower(columnString).contains("not null")) {
            s"(${colNameAndType._1} ${colNameAndType._2}) not null"
          } else s"(${colNameAndType._1} ${colNameAndType._2})"
          val field = colParser.parseSQLOnly(colString, colParser.tableSchemaOpt.run())
          match {
            case Some(fieldSeq) =>
              val field = fieldSeq.head
              val builder = new MetadataBuilder
              builder.withMetadata(field.metadata)
                  .putString("originalSqlType", lower(colNameAndType._2.trim))
              StructField(field.name, field.dataType, field.nullable, builder.build())
            case None => throw
                new IllegalArgumentException("alter statement contains no parsable field")
          }
          alteredSchema = new StructType((schema ++ new StructType(Array(field))).toArray)
          schema = alteredSchema
          assert(schema != null, s"schema for version $versionCnt is null")
          versionCnt = updateMetadataMaps(fqtnKey, alteredSchema, versionCnt)
        } else if (stmt.contains("DROP COLUMN ")) {
          val dropColName = stmt.substring(stmt.indexOf("DROP COLUMN ") + 12).trim
          // loop through schema and delete sruct field matching name and type

          alteredSchema = new StructType(
            schema.toArray.filter(!_.name.equalsIgnoreCase(dropColName)))
          schema = alteredSchema
          assert(schema != null, s"schema for version $versionCnt is null")
          versionCnt = updateMetadataMaps(fqtnKey, alteredSchema, versionCnt)
        } else {
          // do nothing in case of "alter table add constraint"
        }
      })
    })
    logDebug(
      s"""SchemaVsVersionMap:
         |$schemaStructMap
         |
         |TableVsColumnIDsMap:
         |${tableColumnIds.mapValues(_.mkString(", "))}
         |
         |TableVersionMap
         |$versionMap""".stripMargin)
  }

  def getNumBuckets(dbName: String, tableName: String): (Integer, Boolean) = {
    val prName = s"/$dbName/$tableName"
    val memberContainsRegion = memberObject
        .getPrToNumBuckets.containsKey(prName)
    if (memberContainsRegion) {
      (memberObject.getPrToNumBuckets.get(prName), false)
    } else {
      if (memberObject.getReplicatedRegions.contains(prName)) {
        (1, true)
      } else {
        logWarning(s"Number of buckets for $prName not found in ${memberObject.getMember}")
        (0, false)
      }
    }
  }

  private def isReplicated(dbName: String, tableName: String): Boolean =
    memberObject.getReplicatedRegions.contains(s"/$dbName/$tableName")

  def collectViewsAndPrepareCatalog(enableTableCountInUI: Boolean): Unit = synchronized {
    // Send a message to all the servers and locators to send back their
    // respective persistent state information.
    logDebug("Start collecting PersistentStateInRecoveryMode from all the servers/locators.")
    this.enableTableCountInUI = enableTableCountInUI
    val collector = new RecoveryModeResultCollector()
    val msg = new RecoveredMetadataRequestMessage(collector)
    msg.executeFunction()
    val persistentData = collector.getResult
    logDebug(s"Total Number of PersistentStateInRecoveryMode objects received" +
        s" ${persistentData.size()}")
    val itr = persistentData.iterator()
    while (itr.hasNext) {
      val persistentStateObj = itr.next().asInstanceOf[PersistentStateInRecoveryMode]
      locatorMember = if (persistentStateObj.getMember.getVmKind ==
          DistributionManager.LOCATOR_DM_TYPE && locatorMember == null) {
        persistentStateObj.getMember
      }
      else if (persistentStateObj.getMember.getVmKind ==
          DistributionManager.LOCATOR_DM_TYPE && locatorMember != null) {
        persistentStateObj.getMember
      }
      else {
        locatorMember
      }
      persistentObjectMemberMap += persistentStateObj.getMember -> persistentStateObj
      val regionItr = persistentStateObj.getAllRegionViews.iterator()
      while (regionItr.hasNext) {
        val persistentView = regionItr.next()
        val regionPath = persistentView.getRegionPath
        val set = regionViewMapSortedSet.get(regionPath)
        if (set.isDefined) {
          set.get += persistentView
        } else {
          var newset = mutable.SortedSet.empty[RecoveryModePersistentView]
          newset += persistentView
          regionViewMapSortedSet += regionPath -> newset
        }
      }
    }
    assert(locatorMember != null)
    mostRecentMemberObject = persistentObjectMemberMap(locatorMember)
    logDebug(s"The selected PersistentStateInRecoveryMode used for populating" +
        s" the new catalog:\n$mostRecentMemberObject")

    val catalogObjects = mostRecentMemberObject.getCatalogObjects.asScala.map {
      case catFunObj: CatalogFunctionObject =>
        ConnectorExternalCatalog.convertToCatalogFunction(catFunObj)
      case catDBObj: CatalogSchemaObject =>
        ConnectorExternalCatalog.convertToCatalogDatabase(catDBObj)
      case catTabObj: CatalogTableObject =>
        ConnectorExternalCatalog.convertToCatalogTable(new CatalogMetadataDetails()
            .setCatalogTable(catTabObj), SnappyRecoveryParser.sqlConf)._1
      case catParts: Array[AnyRef] =>
        val partitions = catParts(2).asInstanceOf[java.util.List[CatalogPartitionObject]].asScala
            .map(ConnectorExternalCatalog.convertToCatalogPartition).toList
        Array(catParts(0), catParts(1), partitions)
    }
    populateCatalog(catalogObjects)

    // Identify best member to get bucket-related info, etc
    val nonHiveRegionViews = regionViewMapSortedSet.filterKeys(
      !_.startsWith(SystemProperties.SNAPPY_HIVE_METASTORE_PATH))
    if (nonHiveRegionViews.isEmpty) {
      logError("No relevant RecoveryModePersistentViews found.")
      throw new Exception("Cannot start empty cluster in Recovery Mode.")
    }
    val memberToConsider = persistentObjectMemberMap.keySet.toSeq.maxBy(e =>
      persistentObjectMemberMap(e).getPrToNumBuckets.size() +
          persistentObjectMemberMap(e).getReplicatedRegions.size())
    memberObject = persistentObjectMemberMap(memberToConsider)
    logDebug(s"The selected non-Hive PersistentStateInRecoveryMode : $memberObject")
    createSchemasMap()
    createCombinedSortedSet()

    val dbList = recoveryCatalog.listDatabases()
    val allFunctions = dbList.map(db => recoveryCatalog.listFunctions(db, "*").map(
      func => recoveryCatalog.getFunction(db, func)))
    logInfo(
      s"""Catalog contents in recovery mode:
         |Tables:
         |${recoveryCatalog.getAllTables().toString()}
         |Databases:
         |${dbList.map(recoveryCatalog.getDatabase).toString()}
         |Functions:
         |${allFunctions.toString()}
         |""".stripMargin)
  }

  private def createCombinedSortedSet(): Unit = {
    allRecoveredTables.filter(isStoreManagedTable).foreach { table =>
      val tableName = upper(table.identifier.table)
      val dbName = upper(table.database)
      val isColTable = isColumnTable(table)
      val numBuckets = getNumBuckets(dbName, tableName)._1
      var rowRegionPath: String = ""
      var colRegionPath: String = ""
      for (buckNum <- Range(0, numBuckets)) {

        rowRegionPath = getRowRegionPath(dbName, tableName, buckNum)
        colRegionPath = if (isColTable) {
          PartitionedRegionHelper
              .escapePRPath(getColRegionPath(dbName, tableName, buckNum))
              .replaceFirst("___PR__B__", "/__PR/_B__")
        } else ""

        val ssKey = if (colRegionPath.isEmpty) {
          rowRegionPath + "#$"
        } else {
          rowRegionPath + "#$" + colRegionPath
        }
        if (regionViewMapSortedSet.contains(rowRegionPath)) {
          val rowViews = regionViewMapSortedSet(rowRegionPath)
          val colViews =
            if (colRegionPath.nonEmpty) regionViewMapSortedSet(colRegionPath)
            else mutable.SortedSet.empty[RecoveryModePersistentView]

          for (rowView <- rowViews) {
            if (isColTable && colViews.nonEmpty) {
              for (colView <- colViews) {
                if (rowView.getMember.equals(colView.getMember)) {
                  val set = combinedViewsMapSortedSet.get(ssKey)
                  if (set.isDefined) {
                    set.get += new RecoveryModePersistentViewPair(rowView, colView)
                  } else {
                    var newSet = mutable.SortedSet.empty[RecoveryModePersistentViewPair]
                    newSet += new RecoveryModePersistentViewPair(rowView, colView)
                    combinedViewsMapSortedSet += ssKey -> newSet
                  }
                }
              }
            } else {
              // case: when its a row table
              val set = combinedViewsMapSortedSet.get(ssKey)
              if (set.isDefined) {
                set.get += new RecoveryModePersistentViewPair(rowView, null)
              } else {
                var newSet = mutable.SortedSet.empty[RecoveryModePersistentViewPair]
                newSet += new RecoveryModePersistentViewPair(rowView, null)
                combinedViewsMapSortedSet += ssKey -> newSet
              }
            }
          }
        }
      }
    }

    if (isDebugEnabled) {
      logDebug("Printing combinedViewsMapSortedSet")
      for (elem <- combinedViewsMapSortedSet) {
        logDebug(s"bucket:${elem._1} :::::: Set size:${elem._2.size}\n")
        for (viewPair <- elem._2) {
          logDebug(s"Row View : ${viewPair.getRowView.getMember}")
          if (viewPair.getColView ne null) {
            logDebug(s"Col View : ${viewPair.getColView.getMember}")
          }
          logDebug("* ---------- *")
        }
        logDebug("\n\n\n")
      }
    }
  }

  lazy val allRecoveredTables: Seq[CatalogTable] = {
    recoveryCatalog.getAllTables().filter(CatalogObjectType.getTableType(_) match {
      case CatalogObjectType.View | CatalogObjectType.Policy => false
      case _ => true
    })
  }

  /**
   * Populates the external catalog, in recovery mode. Currently table,function and
   * database type of catalog objects is supported.
   *
   * @param catalogObjSeq Sequence of catalog objects to be inserted in the catalog
   */
  private def populateCatalog(catalogObjSeq: Seq[Any]): Unit = {
    val extCatalog = recoveryCatalog
    catalogObjSeq.foreach {
      case catDB: CatalogDatabase =>
        extCatalog.createDatabase(catDB, ignoreIfExists = true)
        logDebug(s"Inserting catalog database: ${catDB.name} in the catalog.")
      case catFunc: CatalogFunction =>
        extCatalog.createFunction(catFunc.identifier.database
            .getOrElse(Constant.DEFAULT_DATABASE), catFunc)
        logDebug(s"Inserting catalog function: ${catFunc.identifier} in the catalog.")
      case catTab: CatalogTable =>
        // change provider to "oplog" for tables having their data in snappy-store so that
        // their data will be read directly from disk
        val tableType = CatalogObjectType.getTableType(catTab)
        val opLogTable =
          if (CatalogObjectType.isTableBackedByRegion(tableType)) {
            if (CatalogObjectType.isColumnTable(tableType)) {
              catTab.copy(provider = Option(SnappyParserConsts.OPLOG_SOURCE),
                storage = catTab.storage.copy(properties = catTab.storage.properties +
                    (SnappyExternalCatalog.IS_COLUMN_TABLE -> "true")))
            } else catTab.copy(provider = Option(SnappyParserConsts.OPLOG_SOURCE))
          } else catTab
        extCatalog.createTable(opLogTable, ignoreIfExists = true)
        logDebug(s"Inserting catalog table: ${catTab.identifier} in the catalog.")
      case catParts: Array[AnyRef] =>
        extCatalog.createPartitions(catParts(0).asInstanceOf[String],
          catParts(1).asInstanceOf[String], catParts(2).asInstanceOf[Seq[CatalogTablePartition]],
          ignoreIfExists = true)
    }
  }
}

object RegionDiskViewOrdering extends Ordering[RecoveryModePersistentView] {
  def compare(element1: RecoveryModePersistentView,
      element2: RecoveryModePersistentView): Int = {
    element2.compareTo(element1)
  }
}

// add currentTime for VIEWs so they are listed at the end of the tables since they don't
// have explicit dependencies on the required tables
final class CatalogTableNode(val table: CatalogTable, currentTime: Long) extends TLinkableAdaptor {
  private[recovery] var timestamp: Long =
    if (table.tableType == CatalogTableType.VIEW) currentTime + table.createTime
    else table.createTime

  private[recovery] var reordered: Boolean = _
}

object SnappyRecoveryParser extends SnappyParser(session = null) {

  private def storeInitDDLs: Rule0 = rule {
    ws ~ passThroughStatementInit ~ EOI
  }

  private def storeDDLs: Rule0 = rule {
    ws ~ (useDatabase ~> ((_: LogicalPlan) => ()) |
        passThroughStatementInit | passThroughStatement(includeIndex = true)) ~ EOI
  }

  private def useDatabaseStatement: Rule1[LogicalPlan] = rule {
    ws ~ useDatabase ~ EOI
  }

  private def createDatabaseWithAuth: Rule1[String] = rule {
    ws ~ CREATE ~ DATABASE ~ ifNotExists ~ identifier ~ AUTHORIZATION ~ ANY.* ~ EOI ~>
        ((_: Boolean, db: String) => db)
  }

  private def checkPlan[T](sqlText: String, parseRule: => Try[T]): Boolean = try {
    parseSQLOnly(sqlText, parseRule)
    true
  } catch {
    case _: ParseException => false
  }

  /**
   * Returns true if the given DDL statement is executed only on the snappydata store
   * at the very start of store `initialization` and makes no changes to Spark's metastore.
   */
  def isStoreInitDDL(sqlText: String): Boolean = checkPlan(sqlText, storeInitDDLs.run())

  /**
   * Returns true if the given DDL statement is executed only on the snappydata store
   * and makes no changes to Spark's metastore.
   */
  def isStoreDDL(sqlText: String): Boolean = checkPlan(sqlText, storeDDLs.run())

  /**
   * Returns true if the given DDL statement is a USE/SET database.
   */
  def isUseDatabase(sqlText: String): Boolean = checkPlan(sqlText, useDatabaseStatement.run())

  /**
   * When recovering from metastore of older releases, the extra AUTHORIZATION clause supported
   * by SnappyData in CREATE DATABASE/SCHEMA does not make any attribute entry in Spark's metastore
   * so this has to be picked separately from the store side.
   */
  def getCreateDatabaseWithAuth(sqlText: String): Option[String] = try {
    Some(parseSQLOnly(sqlText, createDatabaseWithAuth.run()))
  } catch {
    case _: ParseException => None
  }
}
