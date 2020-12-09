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
package org.apache.spark.sql

import java.lang.reflect.InvocationTargetException
import java.sql.{Connection, SQLException, SQLWarning}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.{Calendar, Properties}

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, FinalizeHolder, FinalizeObject}
import com.google.common.cache.{Cache, CacheBuilder}
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.{types => stypes}
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property, QueryHint, SnappyTableStatsProviderService}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.jdbc.{ConnectionConf, ConnectionUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Descending, Exists, ExprId, Expression, GenericRow, ListQuery, ParamLiteral, PlanExpression, ScalarSubquery, SortDirection, TokenLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{Command, Filter, LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils, WrappedInternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExecutedCommandExec, UncacheTableCommand}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingQueryListenerBus
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.hive.{HiveClientUtil, SnappySessionState}
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.internal._
import org.apache.spark.sql.row.{JDBCMutableRelation, SnappyStoreDialect}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryManager}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, ShuffleDependency, SparkContext, SparkEnv}

class SnappySession(_sc: SparkContext) extends SparkSession(_sc)
    with SnappySessionLike with SparkSupport {

  self =>

  // initialize SnappyStoreDialect so that it gets registered

  SnappyStoreDialect.init()

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  val id: Long = SnappySession.newId()

  @transient private lazy val tempCacheIndex = new AtomicInteger(0)

  @transient private var finalizer = new FinalizeSession(this)

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SnappySharedState = SnappyContext.sharedState(sparkContext)

  @transient
  final lazy val contextFunctions: SnappyContextFunctions = SparkSupport.newContextFunctions(self)

  @transient
  lazy val snappySessionState: SnappySessionState = {
    val state = internals.newSnappySessionState(self)
    initStateVars(state)
    contextFunctions.registerSnappyFunctions(state)
    state
  }

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  override lazy val sessionState: SessionState = snappySessionState

  final def sessionCatalog: SnappySessionCatalog = snappySessionState.catalog

  final def externalCatalog: SnappyExternalCatalog =
    snappySessionState.catalog.snappyExternalCatalog

  final def snappyParser: SnappyParser = snappySessionState.snappySqlParser.sqlParser

  SnappyContext.initGlobalSnappyContext(sparkContext, this)
  SparkSession.setActiveSession(this)

  /**
   * A wrapped version of this session in the form of a [[SQLContext]],
   * for backward compatibility.
   */
  @transient
  private[spark] val snappyContext: SnappyContext = new SnappyContext(this)

  /**
   * A wrapped version of this session in the form of a [[SQLContext]],
   * for backward compatibility.
   *
   * @since 2.0.0
   */
  @transient override val sqlContext: SnappyContext = snappyContext

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered
   * functions are isolated, but sharing the underlying [[SparkContext]] and cached data.
   *
   * Note: Other than the [[SparkContext]], all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
   *
   * @group basic
   * @since 2.0.0
   */
  override def newSession(): SnappySession = new SnappySession(sparkContext)

  override private[sql] def cloneSession(): SnappySession = {
    val result = newSession()
    result.cloneSessionState = Some(snappySessionState)
    result.initStateVars(result.sessionState) // force copy of SessionState
    result.snappySessionState.initSnappyStrategies // force add strategies for StreamExecution
    result.cloneSessionState = None
    result
  }

  private[sql] def overrideConfs: Map[String, String] = Map.empty

  override def sql(sqlText: String): DataFrame = {
    try {
      sqInternal(sqlText)
    } catch {
      // fallback to uncached flow for streaming queries
      case ae: AnalysisException
        if ae.message.contains(
          "Queries with streaming sources must be executed with writeStream.start()"
        ) => sqlUncached(sqlText)
    }
  }

   private[sql] def sqInternal(sqlText: String): CachedDataFrame = {
    SnappySession.sqlPlan(this, sqlText)
  }

  @DeveloperApi
  def sqlUncached(sqlText: String): DataFrame = {
    if (planCaching) {
      val initialValue = Property.PlanCaching.get(sessionState.conf)
      planCaching = false
      Property.PlanCaching.set(sessionState.conf, false)
      try {
        super.sql(sqlText)
      } finally {
        planCaching = initialValue
        Property.PlanCaching.set(sessionState.conf, initialValue)
      }
    } else {
      super.sql(sqlText)
    }
  }

  final def prepareSQL(sqlText: String): LogicalPlan = {
    setPreparedQuery(preparePhase = true, None)
    try {
      val logical = snappySessionState.snappySqlParser.parsePlan(sqlText, clearExecutionData = true)
      snappySessionState.executePlan(logical).analyzed
    } finally {
      setPreparedQuery(preparePhase = false, None)
    }
  }

  private[sql] final def executePlan(plan: LogicalPlan, retryCnt: Int = 0): QueryExecution = {
    try {
      val execution = snappySessionState.executePlan(plan)
      execution.assertAnalyzed()
      execution
    } catch {
      case e: AnalysisException =>
        val unresolvedNodes = plan.expressions.filter(
          x => x.isInstanceOf[UnresolvedStar] | x.isInstanceOf[UnresolvedAttribute])
        if (e.getMessage().contains("cannot resolve") && unresolvedNodes.nonEmpty) {
          reAnalyzeForUnresolvedAttribute(plan, e, retryCnt) match {
            case Some(p) => return executePlan(p, retryCnt + 1)
            case None => //
          }
        }
        // in case of connector mode, exception can be thrown if
        // table form is changed (altered) and we have old table
        // object in SnappyExternalCatalog cache
        SnappyContext.getClusterMode(sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            var hasCommand = false
            val relations = plan.collect {
              case _: Command => hasCommand = true; null
              case u: UnresolvedRelation =>
                val tableIdent = sessionCatalog.resolveTableIdentifier(u.tableIdentifier)
                tableIdent.database.get -> tableIdent.table
            }
            if (hasCommand) externalCatalog.invalidateAll()
            else if (relations.nonEmpty) {
              relations.foreach(externalCatalog.invalidate)
            }
            throw e
          case _ =>
            throw e
        }
    }
  }

  // Hack to fix SNAP-2440 ( TODO: Will return after 1.1.0 for a better fix )
  private def reAnalyzeForUnresolvedAttribute(
    originalPlan: LogicalPlan, e: AnalysisException,
    retryCount: Int): Option[LogicalPlan] = {

    if (!e.getMessage().contains("cannot resolve")) return None
    val unresolvedNodes = originalPlan.expressions.filter(
      x => x.isInstanceOf[UnresolvedStar] | x.isInstanceOf[UnresolvedAttribute])
    if (retryCount > unresolvedNodes.size) return None

    val errMsg = e.getMessage().split('\n').map(_.trim.filter(_ >= ' ')).mkString
    val newPlan = originalPlan transformExpressions {
      case us@UnresolvedStar(option) if option.isDefined =>
        val targetString = option.get.mkString(".")
        var matched = false
        errMsg match {
          case SnappySession.unresolvedStarRegex(_, schema, table, _) =>
            if (sessionCatalog.tableExists(tableIdentifier(s"$schema.$table"))) {
              val qname = s"$schema.$table"
              if (qname.equalsIgnoreCase(targetString)) {
                matched = true
              }
            }
          case _ => matched = false
        }
        if (matched) UnresolvedStar(None) else us

      case ua@UnresolvedAttribute(nameparts) =>
        val targetString = nameparts.mkString(".")
        var uqc = ""
        var matched = false
        errMsg match {
          case SnappySession.unresolvedColRegex(_, schema, table, col, _) =>
            if (sessionCatalog.tableExists(tableIdentifier(s"$schema.$table"))) {
              val qname = s"$schema.$table.$col"
              if (qname.equalsIgnoreCase(targetString)) matched = true
              uqc = col
            }
          case _ => matched = false
        }
        if (matched) UnresolvedAttribute(uqc) else ua
    }
    if (!newPlan.equals(originalPlan)) Some(newPlan) else None
  }

  @transient
  private var queryHints = SortedMap.empty[String, String]

  @transient
  private val contextObjects = new ConcurrentHashMap[Any, Any](16, 0.7f, 1)

  @transient private[sql] var currentKey: CachedKey = _
  @transient private[sql] var planCaching: Boolean = _
  @transient private[sql] var partitionPruning: Boolean = _
  @transient private[sql] var disableHashJoin: Boolean = _
  @transient private[sql] var enableHiveSupport: Boolean = _
  @transient private var sqlWarnings: SQLWarning = _
  @transient private[sql] var hiveInitializing: Boolean = _

  private[sql] def initStateVars(sessionState: SessionState): Unit = {
    val conf = sessionState.conf

    // disable LogicalPlan cache since the ExternalCatalog implementations already have
    // a large enough cache and this cache causes lot of trouble with stale data especially
    // in smart connector mode which is already handled by SmartConnectorExternalCatalog
    conf.setConfString("spark.sql.filesourceTableRelationCacheSize", "0")

    planCaching = Property.PlanCaching.get(conf)
    partitionPruning = Property.PartitionPruning.get(conf)
    disableHashJoin = Property.DisableHashJoin.get(conf)
    enableHiveSupport = SnappySession.isHiveSupportEnabled(
      conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION))

    // Call to update Structured Streaming UI Tab
    updateStructuredStreamingUITab(sessionState.streamingQueryManager)
  }

  /**
   * Get the query hints from last [[sql]]/[[sqlUncached]] execution as a case-insensitive map.
   */
  def getLastQueryHints: SortedMap[String, String] = synchronized {
    if (queryHints.isEmpty) SortedMap.empty
    else (SortedMap.newBuilder[String, String] ++= queryHints).result()
  }

  /**
   * Get the index hints from last [[sql]]/[[sqlUncached]] execution as a case-insensitive map.
   */
  def getLastQueryIndexHints: SortedMap[String, String] = synchronized {
    if (queryHints.isEmpty) SortedMap.empty
    else queryHints.filterKeys(_.startsWith(QueryHint.Index.lower))
  }

  /**
   * Get the value of query hint from last [[sql]]/[[sqlUncached]] execution.
   */
  def getLastQueryHint(hint: QueryHint.HintValue): Option[String] = synchronized {
    if (queryHints.isEmpty) None else queryHints.get(hint.lower)
  }

  def addQueryHints(hints: Map[String, String], replace: Boolean): Unit = synchronized {
    if (replace) queryHints = SortedMap.empty
    if (hints.nonEmpty) {
      if (replace) queryHints ++= hints
      else queryHints ++= hints.map(p => Utils.toLowerCase(p._1) -> p._2)
    }
  }

  def getWarnings: SQLWarning = sqlWarnings

  private[sql] def addWarning(warning: SQLWarning): Unit = {
    val warnings = sqlWarnings
    if (warnings eq null) sqlWarnings = warning
    else warnings.setNextWarning(warning)
  }

  def newRddId(): Int = sparkContext.newRddId()

  /**
   * Get a previously registered context object using [[addContextObject]].
   */
  private[sql] def getContextObject[T](key: Any): Option[T] = {
    Option(contextObjects.get(key).asInstanceOf[T])
  }

  /**
   * Get a previously registered CodegenSupport context object
   * by [[addContextObject]].
   */
  private[sql] def getContextObject[T](ctx: CodegenContext, objectType: String,
      key: Any): Option[T] = {
    getContextObject[T](ctx -> (objectType -> key))
  }

  /**
   * Register a new context object for this query.
   */
  private[sql] def addContextObject[T](key: Any, value: T): Unit = {
    contextObjects.put(key, value)
  }

  /**
   * Register a new context object for <code>CodegenSupport</code>.
   */
  private[sql] def addContextObject[T](ctx: CodegenContext, objectType: String,
      key: Any, value: T): Unit = {
    addContextObject(ctx -> (objectType -> key), value)
  }

  /**
   * Remove a context object registered using [[addContextObject]].
   */
  private[sql] def removeContextObject(key: Any): Any = {
    contextObjects.remove(key)
  }

  /**
   * Remove a CodegenSupport context object registered by [[addContextObject]].
   */
  private[sql] def removeContextObject(ctx: CodegenContext, objectType: String,
      key: Any): Unit = {
    removeContextObject(ctx -> (objectType -> key))
  }

  private[sql] def linkPartitionsToBuckets(flag: Boolean): Unit = {
    addContextObject(StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED, flag)
  }

  private[sql] def hasLinkPartitionsToBuckets: Boolean = {
    getContextObject[Boolean](StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED) match {
      case Some(b) => b
      case None => false
    }
  }

  private[sql] def preferPrimaries: Boolean =
    Property.PreferPrimariesInQuery.get(sessionState.conf)

  private[sql] def addFinallyCode(ctx: CodegenContext, code: String): Int = {
    val depth = getContextObject[Int](ctx, "D", "depth").getOrElse(0) + 1
    addContextObject(ctx, "D", "depth", depth)
    addContextObject(ctx, "FIN", "finally" -> depth, code)
    depth
  }

  private[sql] def evaluateFinallyCode(ctx: CodegenContext,
      body: String = "", depth: Int = -1): String = {
    // if no depth given then use the most recent one
    val d = if (depth == -1) {
      getContextObject[Int](ctx, "D", "depth").getOrElse(0)
    } else depth
    if (d <= 1) removeContextObject(ctx, "D", "depth")
    else addContextObject(ctx, "D", "depth", d - 1)

    val key = "finally" -> d
    getContextObject[String](ctx, "FIN", key) match {
      case Some(finallyCode) => removeContextObject(ctx, "F", key)
        if (body.isEmpty) finallyCode
        else {
          s"""
             |try {
             |  $body
             |} finally {
             |   $finallyCode
             |}
          """.stripMargin
        }
      case None => body
    }
  }

  /**
   * Get name of a previously registered class using [[addClass]].
   */
  def getClass(ctx: CodegenContext, baseTypes: Seq[(DataType, Boolean)],
      keyTypes: Seq[(DataType, Boolean)],
      types: Seq[(DataType, Boolean)], multimap: Boolean): Option[(String, String)] = {
    getContextObject[(String, String)](ctx, "C", (baseTypes, keyTypes, types, multimap))
  }

  /**
   * Register code generated for a new class (for <code>CodegenSupport</code>).
   */
  private[sql] def addClass(ctx: CodegenContext,
      baseTypes: Seq[(DataType, Boolean)], keyTypes: Seq[(DataType, Boolean)],
      types: Seq[(DataType, Boolean)], baseClassName: String,
      className: String, multiMap: Boolean): Unit = {
    addContextObject(ctx, "C", (baseTypes, keyTypes, types, multiMap),
      baseClassName -> className)
  }

  /**
   * Register additional [[DictionaryCode]] for a variable in ExprCode.
   */
  private[sql] def addDictionaryCode(ctx: CodegenContext, keyVar: String,
      dictCode: DictionaryCode): Unit =
    addContextObject(ctx, "D", keyVar, dictCode)

  /**
   * Get [[DictionaryCode]] for a previously registered variable in ExprCode
   * using [[addDictionaryCode]].
   */
  def getDictionaryCode(ctx: CodegenContext,
      keyVar: String): Option[DictionaryCode] =
    getContextObject[DictionaryCode](ctx, "D", keyVar)

  /**
   * Register hash variable holding the evaluated hashCode for some variables.
   */
  private[sql] def addHashVar(ctx: CodegenContext, keyVars: Seq[String],
      hashVar: String): Unit = addContextObject(ctx, "H", keyVars, hashVar)

  /**
   * Get hash variable for previously registered variables using [[addHashVar]].
   */
  private[sql] def getHashVar(ctx: CodegenContext,
      keyVars: Seq[String]): Option[String] = getContextObject(ctx, "H", keyVars)

  private[sql] def cachePutInto(doCache: Boolean, updateSubQuery: LogicalPlan,
      table: String): Option[LogicalPlan] = {
    // first acquire the global lock for putInto
    val (schemaName: String, _) =
      JdbcExtendedUtils.getTableWithDatabase(table, session = Some(sqlContext.sparkSession))
    val lockOption = if (Property.SerializeWrites.get(sessionState.conf)) {
      grabLock(table, schemaName, defaultConnectionProps)
    } else None

    var newUpdateSubQuery: Option[LogicalPlan] = None
    try {
      val cachedTable = if (doCache) {
        val tableName = s"snappyDataInternalTempPutIntoCache${tempCacheIndex.incrementAndGet()}"
        val tableIdent = new TableIdentifier(tableName)
        val cacheCommandString = if (currentKey ne null) s"CACHE FOR (${currentKey.sqlText})"
        else s"CACHE FOR (PUT INTO $table <plan>)"
        // use cache table command to display full plan
        val count = SnappyCacheTableCommand(tableIdent, cacheCommandString, Some(updateSubQuery),
          isLazy = false).run(this).head.getLong(0)
        if (count > 0) {
          newUpdateSubQuery = Some(this.table(tableIdent).logicalPlan)
          Some(tableIdent)
        } else {
          dropPutIntoCacheTable(tableIdent)
          None
        }
      } else {
        // assume that there are updates
        newUpdateSubQuery = Some(updateSubQuery)
        None
      }
      addContextObject(SnappySession.CACHED_PUTINTO_LOGICAL_PLAN, cachedTable)
      newUpdateSubQuery
    } finally {
      lockOption match {
        case Some(lock) =>
          logDebug(s"Adding the lock object $lock to the context")
          addContextObject(SnappySession.PUTINTO_LOCK, lock)
        case None => // do nothing
      }
    }
  }

  private def dropPutIntoCacheTable(tableIdent: TableIdentifier): Unit = {
    UncacheTableCommand(tableIdent, ifExists = false).run(this)
    dropTable(tableIdent, ifExists = false, isView = false)
  }

  private[sql] def clearPutInto(): Unit = {
    contextObjects.remove(SnappySession.PUTINTO_LOCK) match {
      case null =>
      case lock => releaseLock(lock)
    }
    contextObjects.remove(SnappySession.CACHED_PUTINTO_LOGICAL_PLAN) match {
      case null =>
      case cachedTable: Option[_] =>
        if (cachedTable.isDefined) {
          dropPutIntoCacheTable(cachedTable.get.asInstanceOf[TableIdentifier])
        }
    }
  }

  private[sql] def clearWriteLockOnTable(): Unit = {
    contextObjects.remove(SnappySession.BULKWRITE_LOCK) match {
      case null =>
      case lock => releaseLock(lock)
    }
  }

  private[sql] def grabLock(table: String, schemaName: String,
      connProperties: ConnectionProperties): Option[Any] = {
    SnappyContext.getClusterMode(sparkContext) match {
      case _: ThinClientConnectorMode =>
        if (!sparkContext.isLocal) {
          SnappyContext.resourceLock.synchronized {
            while (!SnappyContext.executorAssigned && sparkContext.getExecutorIds().isEmpty) {
              if (!SnappyContext.executorAssigned) {
                try {
                  SnappyContext.resourceLock.wait(100)
                }
                catch {
                  case _: InterruptedException =>
                    logWarning("Interrupted while waiting for executor.")
                }
              }
              // Don't expect this case usually unless lots of
              // applications are submitted in parallel
              logDebug(s"grabLock waiting for resources to be " +
                s"allocated ${SnappyContext.executorAssigned}")
            }
          }
        }
        val conn = ConnectionPool.getPoolConnection(table, connProperties.dialect,
          connProperties.poolProps, connProperties.connProps, connProperties.hikariCP)
        var locked = false
        var retrycount = 0
        do {
          try {
            logDebug(s" Going to take lock on server for table $table," +
                s" current Thread ${Thread.currentThread().getId} and " +
              s"app ${sqlContext.sparkContext.appName}")

            val ps = conn.prepareCall(s"VALUES sys.ACQUIRE_REGION_LOCK(?,?)")
            ps.setString(1, SnappySession.WRITE_LOCK_PREFIX + table)
            ps.setInt(2, Property.SerializedWriteLockTimeOut.get(sessionState.conf) * 1000)
            val rs = ps.executeQuery()
            rs.next()
            locked = rs.getBoolean(1)
            ps.close()
            logDebug(s"Took lock on server. for string " +
              s"${SnappySession.WRITE_LOCK_PREFIX + table} and " +
              s"app ${sqlContext.sparkContext.appName}")
          }
          catch {
            case sqle: SQLException =>
              logDebug("Got exception while taking lock", sqle)
              if (sqle.getMessage.contains("Couldn't acquire lock")) {
                throw sqle
              } else {
                if (retrycount == 2) {
                  throw sqle
                }
              }
            case e: Throwable =>
              logDebug("Got exception while taking lock", e)
              if (retrycount == 2) {
                throw e
              }
          }
          finally {
            retrycount = retrycount + 1
            // conn.close()
          }
        } while (!locked)
        Some((conn, new TableIdentifier(table, Some(schemaName))))
      case _ =>
        logDebug(s"Taking lock in " +
            s" ${Thread.currentThread().getId} and " +
          s"app ${sqlContext.sparkContext.appName}")
        val regionLock = PartitionedRegion.getRegionLock(SnappySession.WRITE_LOCK_PREFIX + table,
          GemFireCacheImpl.getExisting)
        regionLock.lock(Property.SerializedWriteLockTimeOut.get(sessionState.conf) * 1000)
        Some(regionLock)
    }
  }

  private[sql] def releaseLock(lock: Any): Unit = {
    logInfo(s"Releasing the lock : $lock")
    lock match {
      case lock: PartitionedRegion.RegionLock =>
        if (lock != null) {
          logInfo(s"Going to unlock the lock object bulkOp $lock and " +
            s"app ${sqlContext.sparkContext.appName}")
          lock.unlock()
        }
      case (conn: Connection, id: TableIdentifier) =>
        var unlocked = false
        try {
          logDebug(s"Going to unlock the lock on the server. ${id.table} for " +
            s"app ${sqlContext.sparkContext.appName}")
          val ps = conn.prepareStatement(s"VALUES sys.RELEASE_REGION_LOCK(?)")
          ps.setString(1, SnappySession.WRITE_LOCK_PREFIX + id.table)
          val rs = ps.executeQuery()
          rs.next()
          unlocked = rs.getBoolean(1)
          ps.close()
        } catch {
          case t: Throwable =>
            logWarning(s"Caught exception while unlocking the $lock", t)
            throw t
        }
        finally {
          conn.close()
        }
    }
  }

  private[sql] def clearContext(): Unit = synchronized {
    clearPutInto()
    clearWriteLockOnTable()
    contextObjects.clear()
    sqlWarnings = null
  }

  private[sql] def clearQueryData(): Unit = synchronized {
    queryHints = SortedMap.empty
  }

  def clearPlanCache(): Unit = synchronized {
    SnappySession.clearSessionCache(id)
  }


  def clear(): Unit = synchronized {
    clearContext()
    clearQueryData()
    clearPlanCache()
    contextFunctions.clear()
  }

  /** Close the session which will be unusable after this call. */
  override def close(): Unit = synchronized {
    clear()
    externalCatalog match {
      case c: SmartConnectorExternalCatalog => c.close()
      case _ => // nothing for global embedded catalog
    }
    val tcb = ToolsCallbackInit.toolsCallback
    if (tcb ne null) {
      tcb.closeAndClearScalaInterpreter(id)
    }

    val finalizer = this.finalizer
    if (finalizer ne null) {
      finalizer.doFinalize()
      finalizer.clearAll()
      this.finalizer = null
    }
  }

  /**
   * :: DeveloperApi ::
   *
   * @todo do we need this anymore? If useful functionality, make this
   *       private to sql package ... SchemaDStream should use the data source
   *       API?
   *       Tagging as developer API, for now
   * @param stream
   * @param aqpTables
   * @param transformer
   * @param v
   * @tparam T
   * @return
   */
  @DeveloperApi
  def saveStream[T](stream: DStream[T],
      aqpTables: Seq[String],
      transformer: Option[RDD[T] => RDD[Row]])(implicit v: TypeTag[T]): Unit = {
    val transform = transformer match {
      case Some(x) => x
      case None => if (!(v.tpe =:= typeOf[Row])) {
        // check if the stream type is already a Row
        throw new IllegalStateException(" Transformation to Row type needs to be supplied")
      } else {
        null
      }
    }
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = if (transform != null) {
        transform(rdd)
      } else {
        rdd.asInstanceOf[RDD[Row]]
      }
      contextFunctions.collectSamples(rddRows, aqpTables, time.milliseconds)
    })
  }

  def tableIdentifier(table: String, resolve: Boolean = false): TableIdentifier =
    SnappySession.tableIdentifier(table, sessionCatalog, resolve)

  /**
   * Append dataframe to cache table in Spark.
   *
   * @param df
   * @param table
   * @param storageLevel default storage level is MEMORY_AND_DISK
   * @return @todo -> return type?
   */
  @DeveloperApi
  def appendToTempTableCache(df: DataFrame, table: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {
    val tableIdent = tableIdentifier(table)
    if (!sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException(s"Schema specified for temporary table '$tableIdent'")
    }
    val plan = sessionCatalog.lookupRelation(tableIdent)
    // cache the new DataFrame
    df.persist(storageLevel)
    // trigger an Action to materialize 'cached' batch
    if (df.count() > 0) {
      // create a union of the two plans and store that in catalog
      val union = Union(plan, df.logicalPlan)
      if (sessionCatalog.isLocalTemporaryView(tableIdent)) {
        sessionCatalog.createTempView(table, union, overrideIfExists = true)
      } else {
        sessionCatalog.createGlobalTempView(table, union, overrideIfExists = true)
      }
    }
  }

  /**
   * Empties the contents of the table without deleting the catalog entry.
   *
   * @param table    full table name to be truncated
   * @param ifExists attempt truncate only if the table exists
   */
  def truncateTable(table: String, ifExists: Boolean = false): Unit = {
    sessionState.executePlan(TruncateManagedTableCommand(ifExists, tableIdentifier(table))).toRdd
  }

  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = {
    val encoder = encoderFor[T]
    val output = encoder.schema.toAttributes
    val c = encoder.clsTag.runtimeClass
    val isFlat = !(classOf[Product].isAssignableFrom(c) ||
        classOf[DefinedByConstructorParams].isAssignableFrom(c))
    val plan = EncoderPlan[T](data, encoder, isFlat, output)(self)
    Dataset[T](self, plan)
  }

  /**
   * Creates a [[DataFrame]] from an RDD[Row]. User can specify whether
   * the input rows should be converted to Catalyst rows.
   */
  override private[sql] def createDataFrame(
      rowRDD: RDD[Row],
      schema: StructType,
      needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val catalystRows = if (needsConversion) {
      val encoder = RowEncoder(schema)
      rowRDD.map {
        case r: WrappedInternalRow => r.internalRow
        case r => encoder.toRow(r)
      }
    } else {
      rowRDD.map(r => InternalRow.fromSeq(r.toSeq))
    }
    internalCreateDataFrame(catalystRows.setName(rowRDD.name), schema)
  }

  /**
   * Create a stratified sample table.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      samplingOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTableInternal(tableIdentifier(tableName), SnappyContext.SAMPLE_SOURCE,
      userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isExternal = false)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any, or null
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String, samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable),
      samplingOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create a stratified sample table.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any
   * @param schema          schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      schema: StructType,
      samplingOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    createTableInternal(tableIdentifier(tableName), SnappyContext.SAMPLE_SOURCE,
      Some(JdbcExtendedUtils.normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isExternal = false)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any, or null
   * @param schema          schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String, schema: StructType,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable), schema,
      samplingOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: Option[String],
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    createTableInternal(tableIdentifier(topKName), SnappyContext.TOPK_SOURCE,
      Some(JdbcExtendedUtils.normalizeSchema(inputDataSchema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) +
          ("key" -> keyColumnName), isExternal = false)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * Java friendly api.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any, or null
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: String,
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, Option(baseTable), keyColumnName,
      inputDataSchema, topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: Option[String],
      keyColumnName: String, topkOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTableInternal(tableIdentifier(topKName), SnappyContext.TOPK_SOURCE,
      userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) +
          ("key" -> keyColumnName), isExternal = false)
  }

  /**
   * Create approximate structure to query top-K with time series support. Java
   * friendly api.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any, or null
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: String,
      keyColumnName: String, topkOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, Option(baseTable), keyColumnName,
      topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed table. Any of the table types
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline,
   *   "column", Map("buckets" -> "29"))
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      schemaDDL = None, if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isExternal = false)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      schemaDDL = None, if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isExternal = false)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline,
   *   "column", Map("buckets" -> "29"))
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, options.asScala.toMap, allowExisting)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createExternalTable(tableName, provider, options.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   * val props = Map.empty[String, String]
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * snappyContext.createTable(tableName, "column", dataDF.schema, props)
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider,
      Some(JdbcExtendedUtils.normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, options, isExternal = false)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider, Some(schema), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, options, isExternal = true)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   *    case class Data(col1: Int, col2: Int, col3: Int)
   *    val props = Map.empty[String, String]
   *    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   *    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   *    val dataDF = snc.createDataFrame(rdd)
   *    snappyContext.createTable(tableName, "column", dataDF.schema, props)
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schema, options.asScala.toMap, allowExisting)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createExternalTable(tableName, provider, schema, options.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed JDBC table which takes a free format ddl
   * string. The ddl string should adhere to syntax of underlying JDBC store.
   * SnappyData ships with inbuilt JDBC store, which can be accessed by
   * Row format data store. The option parameter can take connection details.
   *
   * {{{
   *    val props = Map(
   *      "url" -> s"jdbc:derby:$path",
   *      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   *      "poolImpl" -> "tomcat",
   *      "user" -> "app",
   *      "password" -> "app"
   *    )
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * }}}
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter API.
   *
   * e.g.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.insertInto("jdbcTable")
   *
   * }}}
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param schemaDDL     Table schema as a string interpreted by provider
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    var schemaStr = schemaDDL.trim
    if (schemaStr.charAt(0) != '(') {
      schemaStr = "(" + schemaStr + ")"
    }
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      Some(schemaStr), if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isExternal = false)
  }

  /**
   * Creates a SnappyData managed JDBC table which takes a free format ddl
   * string. The ddl string should adhere to syntax of underlying JDBC store.
   * SnappyData ships with inbuilt JDBC store, which can be accessed by
   * Row format data store. The option parameter can take connection details.
   *
   * {{{
   *    val props = Map(
   *      "url" -> s"jdbc:derby:$path",
   *      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   *      "poolImpl" -> "tomcat",
   *      "user" -> "app",
   *      "password" -> "app"
   *    )
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * }}}
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter API.
   *
   * e.g.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.insertInto("jdbcTable")
   *
   * }}}
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param schemaDDL     Table schema as a string interpreted by provider
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schemaDDL, options.asScala.toMap,
      allowExisting)
  }

  // scalastyle:off
  /**
   * Create a table with given name, provider, optional schema DDL string, optional schema.
   * and other options.
   */
  private[sql] def createTableInternal(
      tableIdent: TableIdentifier,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isExternal: Boolean,
      properties: Map[String, String] = Map.empty[String, String],
      partitionColumns: Seq[String] = Nil,
      bucketSpec: Option[BucketSpec] = None,
      query: Option[LogicalPlan] = None,
      comment: Option[String] = None,
      location: Option[String] = None): DataFrame = {
    // scalastyle:on

    val providerIsBuiltIn = SnappyContext.isBuiltInProvider(provider)
    if (providerIsBuiltIn) {
      if (isExternal) {
        throw new AnalysisException(s"CREATE EXTERNAL TABLE or createExternalTable API " +
            s"used for inbuilt provider '$provider'")
      }
      // for builtin tables, never use partitionSpec or bucketSpec since that has different
      // semantics and implies support for add/drop/recover partitions which is not possible
      if (partitionColumns.nonEmpty) {
        throw new AnalysisException(s"CREATE TABLE ... USING '$provider' does not support " +
            "PARTITIONED BY clause.")
      }
      if (bucketSpec.isDefined) {
        throw new AnalysisException(s"CREATE TABLE ... USING '$provider' does not support " +
            s"CLUSTERED BY clause. Use '${ExternalStoreUtils.PARTITION_BY}' as an option.")
      }
    }
    // check for permissions in the schema which should be done before the session catalog
    // createTable call since store table will be created by that time
    val resolvedIdentifier = sessionCatalog.resolveTableIdentifier(tableIdent)
    sessionCatalog.checkDbPermission(resolvedIdentifier.database.get, resolvedIdentifier.table,
      defaultUser = null, ignoreIfNotExists = true)

    val schema = userSpecifiedSchema match {
      case Some(s) =>
        if (query.isDefined) {
          throw new AnalysisException(
            "Schema may not be specified in a Create Table As Select (CTAS) statement")
        }
        s
      // CreateTable plan execution will resolve schema before adding to externalCatalog
      case None => new StructType()
    }
    var fullOptions = schemaDDL match {
      case None => options
      case Some(ddl) =>
        // check that the DataSource should implement ExternalSchemaRelationProvider
        if (!ExternalStoreUtils.isExternalSchemaRelationProvider(provider, sessionState.conf)) {
          throw new AnalysisException(s"Provider '$provider' should implement " +
              s"ExternalSchemaRelationProvider to use a custom schema string in CREATE TABLE")
        }
        JdbcExtendedUtils.addSplitProperty(ddl,
          SnappyExternalCatalog.SCHEMADDL_PROPERTY, options,
          sparkContext.conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)).toMap
    }
    // add baseTable for colocated table
    val parameters = new CaseInsensitiveMutableHashMap[String](fullOptions)
    if (!parameters.contains(SnappyExternalCatalog.BASETABLE_PROPERTY)) {
      parameters.get(StoreUtils.COLOCATE_WITH) match {
        case None =>
        case Some(b) => fullOptions += SnappyExternalCatalog.BASETABLE_PROPERTY ->
            sessionCatalog.resolveExistingTable(b).unquotedString
      }
    }
    // if there is no path option for external DataSources, then mark as MANAGED except for JDBC
    if (location.isDefined) {
      if (parameters.contains("path")) {
        throw new ParseException(
          "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
              "you can only specify one of them.")
      } else {
        fullOptions += "path" -> location.get
      }
    }
    val storage = DataSource.buildStorageFormatFromOptions(fullOptions)
    val tableType = if (!providerIsBuiltIn && storage.locationUri.isEmpty &&
        !Utils.toLowerCase(provider).contains("jdbc")) {
      CatalogTableType.MANAGED
    } else CatalogTableType.EXTERNAL
    val tableDesc = CatalogTable(
      identifier = resolvedIdentifier,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = bucketSpec,
      properties = properties,
      comment = comment)
    val plan = CreateTable(tableDesc, mode, query.map(MarkerForCreateTableAsSelect))
    sessionState.executePlan(plan).toRdd
    val df = table(resolvedIdentifier)
    val relation = df.queryExecution.analyzed.collectFirst {
      case l: LogicalRelation => l.relation
    }
    contextFunctions.postRelationCreation(relation)
    df
  }

  private[sql] def addBaseTableOption(baseTable: Option[String],
      options: Map[String, String]): Map[String, String] = baseTable match {
    case Some(t) => options + (SnappyExternalCatalog.BASETABLE_PROPERTY ->
        sessionCatalog.resolveExistingTable(t).unquotedString)
    case _ => options
  }

  /**
   * Drop a table created by a call to createTable or createExternalTable.
   *
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    dropTable(tableIdentifier(tableName), ifExists, isView = false)

  /**
   * Drop a view.
   *
   * @param viewName name of the view to be dropped
   * @param ifExists attempt drop only if the view exists
   */
  def dropView(viewName: String, ifExists: Boolean = false): Unit =
    dropTable(tableIdentifier(viewName), ifExists, isView = true)

  /**
   * Drop a table created by a call to createTable or createExternalTable.
   *
   * @param tableIdent table to be dropped
   * @param ifExists   attempt drop only if the table exists
   */
  private[sql] def dropTable(tableIdent: TableIdentifier, ifExists: Boolean,
      isView: Boolean): Unit = {
    val plan = DropTableOrViewCommand(tableIdent, ifExists, isView, purge = false)
    sessionState.executePlan(plan).toRdd
  }

  /**
   * Drop a SnappyData Policy created by a call to [[createPolicy]].
   *
   * @param policyName Policy to be dropped
   * @param ifExists   attempt drop only if the Policy exists
   */
  def dropPolicy(policyName: String, ifExists: Boolean = false): Unit =
    dropPolicy(tableIdentifier(policyName), ifExists)

  /**
   * Drop a SnappyData Policy created by a call to [[createPolicy]].
   *
   * @param policyIdent Policy to be dropped
   * @param ifExists    attempt drop only if the Policy exists
   */
  private[sql] def dropPolicy(policyIdent: TableIdentifier, ifExists: Boolean): Unit = {
    try {
      dropTable(policyIdent, ifExists, isView = false)
    } catch {
      case _: NoSuchTableException if !ifExists =>
        throw new PolicyNotFoundException(policyIdent.database.getOrElse(getCurrentSchema),
          policyIdent.table)
    }
  }

  def alterTable(tableName: String, isAddColumn: Boolean, column: StructField,
      extensions: String): Unit = {
    val tableIdent = tableIdentifier(tableName)
    alterTable(tableIdent, isAddColumn, column, extensions)
  }

  private[sql] def alterTable(tableIdent: TableIdentifier, isAddColumn: Boolean,
      column: StructField, extensions: String): Unit = {
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE not supported for temporary tables")
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[AlterableRelation] =>
        val ar = lr.relation.asInstanceOf[AlterableRelation]
        ar.alterTable(tableIdent, isAddColumn, column, extensions)
        val metadata = sessionCatalog.getTableMetadata(tableIdent)
        sessionCatalog.alterTable(metadata.copy(schema = lr.relation.schema))
      case _ => throw new AnalysisException(
        s"ALTER TABLE ${tableIdent.unquotedString} supported only for row tables")
    }
  }

  private[sql] def alterTableToggleRLS(table: TableIdentifier, enableRls: Boolean): Unit = {
    val tableIdent = sessionCatalog.resolveTableIdentifier(table)
    val plan = sessionCatalog.resolveRelation(tableIdent)
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
          "not supported for temporary tables")
    }

    SnappyContext.getClusterMode(sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
        throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
            "not supported for smart connector mode")
      case _ =>
    }

    plan match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[RowLevelSecurityRelation] =>
        lr.relation.asInstanceOf[RowLevelSecurityRelation].enableOrDisableRowLevelSecurity(
            tableIdent, enableRls)
        externalCatalog.invalidateCaches(tableIdent.database.get -> tableIdent.table :: Nil)
      case _ =>
        throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
            s"not supported for ${tableIdent.unquotedString}")
    }
  }

  private[sql] def alterTableMisc(tableIdent: TableIdentifier, sql: String): Unit = {
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE not supported for temporary tables")
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[JDBCMutableRelation] =>
        lr.relation.asInstanceOf[JDBCMutableRelation].executeUpdate(sql,
          JdbcExtendedUtils.toUpperCase(getCurrentSchema))
      case _ => throw new AnalysisException(
        s"ALTER TABLE ${tableIdent.unquotedString} variant only supported for row tables")
    }
  }

  /**
   * Set current schema for the session.
   *
   * @param schema schema name which goes in the catalog
   */
  def setCurrentSchema(schema: String): Unit = setCurrentSchema(schema, createIfNotExists = false)

  /**
   * Set current schema for the session.
   *
   * @param schema            schema name which goes in the catalog
   * @param createIfNotExists create the schema if it does not exist
   */
  private[sql] def setCurrentSchema(schema: String, createIfNotExists: Boolean): Unit = {
    val schemaName = sessionCatalog.formatDatabaseName(schema)
    if (schemaName != getCurrentSchema) {
      if (createIfNotExists) {
        sessionCatalog.createDatabase(schemaName, ignoreIfExists = true, createInStore = false)
      }
      sessionCatalog.setCurrentDatabase(schemaName, force = true)
    }
  }

  def getCurrentSchema: String = sessionCatalog.getCurrentDatabase

  /**
   * Create an index on a table.
   *
   * @param indexName    Index name which goes in the catalog
   * @param baseTable    Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created along with the
   *                     sorting direction.
   * @param sortOrders   Sorting direction for indexColumns. The direction of index will be
   *                     ascending if value is true and descending when value is false.
   *                     The values in this list must exactly match indexColumns list.
   *                     Direction can be specified as null in which case ascending is used.
   * @param options      Options for indexes. For e.g.
   *                     column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                     row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: java.util.List[String],
      sortOrders: java.util.List[java.lang.Boolean],
      options: java.util.Map[String, String]): Unit = {

    val numIndexes = indexColumns.size()
    if (numIndexes != sortOrders.size()) {
      throw new IllegalArgumentException("Number of index columns should match the sorting orders")
    }
    val indexCols = for (i <- 0 until numIndexes) yield {
      val sortDirection = sortOrders.get(i) match {
        case null => None
        case java.lang.Boolean.TRUE => Some(Ascending)
        case java.lang.Boolean.FALSE => Some(Descending)
      }
      indexColumns.get(i) -> sortDirection
    }

    createIndex(indexName, baseTable, indexCols, options.asScala.toMap)
  }

  /**
   * Create an index on a table.
   *
   * @param indexName    Index name which goes in the catalog
   * @param baseTable    Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created with the
   *                     direction of sorting. Direction can be specified as None.
   * @param options      Options for indexes. For e.g.
   *                     column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                     row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {
    createIndex(tableIdentifier(indexName), tableIdentifier(baseTable), indexColumns, options)
  }

  private[sql] def createPolicy(policyName: TableIdentifier, tableName: TableIdentifier,
      policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      currentUser: String, filterStr: String, filter: BypassRowLevelSecurity): Unit = {

    /*
    if (!SecurityUtils.allowPolicyOp(currentUser, tableName, this)) {
      throw new SQLException("Only Table Owner can create the policy", "01548", null)
    }
    */

    if (!policyFor.equalsIgnoreCase(SnappyParserConsts.SELECT.lower)) {
      throw new AnalysisException("Currently Policy only For Select is supported")
    }

    /*
    if (isTargetExternalRelation) {
      val targetAttributes = this.sessionState.catalog.lookupRelation(tableName).output
      def checkForValidFilter(filter: BypassRowLevelSecurity): Unit = {
        def checkExpression(expression: Expression): Unit = {
          expression match {
            case _: Attribute =>  // ok
            case _: Literal =>  // ok
            case _: TokenizedLiteral => // ok
            case br: BinaryComparison => {
              checkExpression(br.left)
              checkExpression(br.right)
            }
            case logicalOr(left, right) => {
              checkExpression(left)
              checkExpression(right)
            }
            case logicalAnd(left, right) => {
              checkExpression(left)
              checkExpression(right)
            }
            case logicalIn(value, list) => {
              checkExpression(value)
              list.foreach(checkExpression(_))
            }
            case _ => // for any other type of expression
              // it should not contain any attribute of target external relation
              expression.foreach(x => x match {
                case ne: NamedExpression => targetAttributes.find(_.exprId == ne.exprId).
                    foreach( _ => throw new AnalysisException("Filter for external " +
                        "relation cannot have functions " +
                        "or dependent subquery involving external table's attribute") )
              })

          }
        }
        checkExpression(filter.child.condition)

      }
      checkForValidFilter(filter)

    }
    */

    sessionCatalog.createPolicy(policyName, tableName, policyFor, applyTo, expandedPolicyApplyTo,
      currentUser, filterStr)
  }

  /**
   * Create an index on a table.
   */
  private[sql] def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {

    if (indexIdent.database != tableIdent.database) {
      throw new AnalysisException(
        s"Index and table have different schemas " +
            s"specified ${indexIdent.database} and ${tableIdent.database}")
    }
    if (!sessionCatalog.tableExists(tableIdent)) {
      throw new AnalysisException(
        s"Could not find $tableIdent in catalog")
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[IndexableRelation] =>
        lr.relation.asInstanceOf[IndexableRelation].createIndex(indexIdent,
          tableIdent,
          indexColumns,
          options)

      case _ => throw new AnalysisException(
        s"$tableIdent is not an indexable table")
    }
  }

  private[sql] def getIndexTable(indexIdent: TableIdentifier): TableIdentifier = {
    val schema = indexIdent.database match {
      case None => Some(getCurrentSchema)
      case s => s
    }
    TableIdentifier(Constant.COLUMN_TABLE_INDEX_PREFIX + indexIdent.table, schema)
  }

  private def constructDropSQL(indexName: String,
      ifExists: Boolean): String = {
    val ifExistsClause = if (ifExists) " IF EXISTS" else ""
    s"DROP INDEX$ifExistsClause ${JdbcExtendedUtils.quotedName(indexName)}"
  }

  /**
   * Drops an index on a table
   *
   * @param indexName Index name which goes in catalog
   * @param ifExists  Drop if exists, else exit gracefully
   */
  def dropIndex(indexName: String, ifExists: Boolean): Unit = {
    dropIndex(tableIdentifier(indexName), ifExists)
  }

  /**
   * Drops an index on a table
   */
  private[sql] def dropIndex(indexName: TableIdentifier, ifExists: Boolean): Unit = {
    val indexIdent = getIndexTable(indexName)
    // Since the index does not exist in catalog, it may be a row table index.
    if (!sessionCatalog.tableExists(indexIdent)) {
      dropRowStoreIndex(sessionCatalog.resolveTableIdentifier(indexName).unquotedString, ifExists)
    } else {
      sessionCatalog.resolveRelation(indexIdent) match {
        case lr: LogicalRelation if lr.relation.isInstanceOf[IndexColumnFormatRelation] =>
          // Remove the index from the bse table props
          val baseTableIdent = tableIdentifier(
            lr.relation.asInstanceOf[IndexColumnFormatRelation].baseTable.get)
          sessionCatalog.resolveRelation(baseTableIdent) match {
            case lr: LogicalRelation if lr.relation.isInstanceOf[ColumnFormatRelation] =>
              val cr = lr.relation.asInstanceOf[ColumnFormatRelation]
              cr.dropIndex(indexIdent, baseTableIdent, ifExists)
            case _ => throw new AnalysisException(
              s"No index ${indexName.unquotedString} on ${baseTableIdent.unquotedString}")
          }

        case _ => if (!ifExists) {
          throw new AnalysisException(s"No index found for ${indexName.unquotedString}")
        }
      }
    }
  }

  /**
   * creates a thin connection to snappydata and returns it
   * @return connection to snappydata
   */
  private def getConnection(table: String): Connection = {
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(this), ExternalStoreUtils.emptyCIMutableMap)
    val jdbcOptions = new JDBCOptions(connProperties.url, table,
      connProperties.connProps.asScala.toMap)
    JdbcUtils.createConnectionFactory(jdbcOptions)()
  }

  private def dropRowStoreIndex(indexName: String, ifExists: Boolean): Unit = {
    val conn = getConnection(indexName)
    try {
      val sql = constructDropSQL(indexName, ifExists)
      JdbcExtendedUtils.executeUpdate(sql, conn)
    } catch {
      case sqle: SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      conn.commit()
      conn.close()
    }
  }

  /**
   * Insert one or more [[org.apache.spark.sql.Row]] into an existing table
   * {{{
   *        snSession.insert(tableName, dataDF.collect(): _*)
   * }}}
   * If insert is on a column table then a row insert can trigger an overflow
   * to column store form row buffer. If the overflow fails due to some condition like
   * low memory , then the overflow fails and exception is thrown,
   * but row buffer values are kept as it is. Any user level counter of number of rows inserted
   * might be invalid in such a case.
   *
   * @param tableName table name for the insert operation
   * @param rows      list of rows to be inserted into the table
   * @return number of rows inserted
   */
  @DeveloperApi
  def insert(tableName: String, rows: Row*): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[RowInsertableRelation] =>
        lr.relation.asInstanceOf[RowInsertableRelation].insert(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  /**
   * Insert one or more [[org.apache.spark.sql.Row]] into an existing table
   * {{{
   *        java.util.ArrayList[java.util.ArrayList[_] rows = ...    *
   *         snSession.insert(tableName, rows)
   * }}}
   *
   * @param tableName table name for the insert operation
   * @param rows      list of rows to be inserted into the table
   * @return number of rows successfully put
   * @return number of rows inserted
   */
  @Experimental
  def insert(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    val convertedRowSeq: Seq[Row] = rows.asScala.map(row => convertListToRow(row))
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[RowInsertableRelation] =>
        lr.relation.asInstanceOf[RowInsertableRelation].insert(convertedRowSeq)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  /**
   * Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
   * {{{
   *        snSession.put(tableName, dataDF.collect(): _*)
   * }}}
   *
   * @param tableName table name for the put operation
   * @param rows      list of rows to be put on the table
   * @return number of rows successfully put
   */
  @DeveloperApi
  def put(tableName: String, rows: Row*): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[RowPutRelation] =>
        lr.relation.asInstanceOf[RowPutRelation].put(rows)
        case _ => throw new AnalysisException(
        s"$tableName is not a row upsertable table")
    }
  }

  /**
   * Update all rows in table that match passed filter expression
   * {{{
   *   snappyContext.update("jdbcTable", "ITEMREF = 3" , Row(99) , "ITEMREF" )
   * }}}
   *
   * @param tableName        table name which needs to be updated
   * @param filterExpr       SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues  A single Row containing all updated column
   *                         values. They MUST match the updateColumn list
   *                         passed
   * @param updateColumns    List of all column names being updated
   * @return
   */
  @DeveloperApi
  def update(tableName: String, filterExpr: String, newColumnValues: Row,
      updateColumns: String*): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[UpdatableRelation] =>
        lr.relation.asInstanceOf[UpdatableRelation].update(filterExpr,
          newColumnValues, updateColumns)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  /**
   * Update all rows in table that match passed filter expression
   * {{{
   *   snappyContext.update("jdbcTable", "ITEMREF = 3" , Row(99) , "ITEMREF" )
   * }}}
   *
   * @param tableName       table name which needs to be updated
   * @param filterExpr      SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues A list containing all the updated column
   *                        values. They MUST match the updateColumn list
   *                        passed
   * @param updateColumns   List of all column names being updated
   * @return
   */
  @Experimental
  def update(tableName: String, filterExpr: String, newColumnValues: java.util.ArrayList[_],
      updateColumns: java.util.ArrayList[String]): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[UpdatableRelation] =>
        lr.relation.asInstanceOf[UpdatableRelation].update(filterExpr,
          convertListToRow(newColumnValues), updateColumns.asScala)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  /**
   * Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
   * {{{
   *        java.util.ArrayList[java.util.ArrayList[_] rows = ...    *
   *         snSession.put(tableName, rows)
   * }}}
   *
   * @param tableName table name for the put operation
   * @param rows      list of rows to be put on the table
   * @return number of rows successfully put
   */
  @Experimental
  def put(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[RowPutRelation] =>
        lr.relation.asInstanceOf[RowPutRelation].put(
          rows.asScala.map(row => convertListToRow(row)))
      case _ => throw new AnalysisException(
        s"$tableName is not a row upsertable table")
    }
  }


  /**
   * Delete all rows in table that match passed filter expression
   *
   * @param tableName  table name
   * @param filterExpr SQL WHERE criteria to select rows that will be updated
   * @return number of rows deleted
   */
  @DeveloperApi
  def delete(tableName: String, filterExpr: String): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case lr: LogicalRelation if lr.relation.isInstanceOf[DeletableRelation] =>
        lr.relation.asInstanceOf[DeletableRelation].delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  private def convertListToRow(row: java.util.ArrayList[_]): Row = {
    val rowAsArray: Array[Any] = row.asScala.toArray
    new GenericRow(rowAsArray)
  }

  private[sql] def defaultConnectionProps: ConnectionProperties =
    ExternalStoreUtils.validateAndGetAllProps(Some(this), ExternalStoreUtils.emptyCIMutableMap)

  private[sql] def defaultPooledConnection(name: String): java.sql.Connection =
    ConnectionUtil.getPooledConnection(name, new ConnectionConf(defaultConnectionProps))

  /**
   * Fetch the topK entries in the Approx TopK synopsis for the specified
   * time interval. See _createTopK_ for how to create this data structure
   * and associate this to a base table (i.e. the full data set). The time
   * interval specified here should not be less than the minimum time interval
   * used when creating the TopK synopsis.
   *
   * @todo provide an example and explain the returned DataFrame. Key is the
   *       attribute stored but the value is a struct containing
   *       count_estimate, and lower, upper bounds? How many elements are
   *       returned if K is not specified?
   * @param topKName  - The topK structure that is to be queried.
   * @param startTime start time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                  If passed as null, oldest interval is considered as the start interval.
   * @param endTime   end time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                  If passed as null, newest interval is considered as the last interval.
   * @param k         Optional. Number of elements to be queried.
   *                  This is to be passed only for stream summary
   * @return returns the top K elements with their respective frequencies between two time
   */
  def queryApproxTSTopK(topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1): DataFrame =
    contextFunctions.queryTopK(topKName, startTime, endTime, k)

  def queryApproxTSTopK(topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    contextFunctions.queryTopK(topK, startTime, endTime, k)

  def isPreparePhase: Boolean = snappyParser.isPreparePhase

  def setPreparedQuery(preparePhase: Boolean, paramSet: Option[ParameterValueSet]): Unit =
    snappyParser.setPreparedQuery(preparePhase, paramSet)

  def setPreparedParamsTypeInfo(info: Array[Int]): Unit =
    snappyParser.setPrepareParamsTypesInfo(info)

  private[sql] def getParameterValue(questionMarkCounter: Int,
      pvs: Any, preparedParamsTypesInfo: Option[Array[Int]]): (Any, DataType) = {
    val parameterValueSet = pvs.asInstanceOf[ParameterValueSet]
    if (questionMarkCounter > parameterValueSet.getParameterCount) {
      assert(assertion = false, s"For Prepared Statement, Got more number of" +
          s" placeholders = $questionMarkCounter" +
          s" than given number of parameter" +
          s" constants = ${parameterValueSet.getParameterCount}")
    }
    val dvd = parameterValueSet.getParameter(questionMarkCounter - 1)
    val scalaTypeVal = SnappySession.getValue(dvd)
    val storeType = dvd.getTypeFormatId
    val (storePrecision, storeScale) = dvd match {
      case _: stypes.SQLDecimal =>
        val index = (questionMarkCounter - 1) * 4 + 1
        // actual precision and scale of the target column
        preparedParamsTypesInfo match {
          case None => (-1, -1)
          case Some(a) => (a(index + 1), a(index + 2))
        }

      case _ => (-1, -1)
    }
    (scalaTypeVal, SnappySession.getDataType(storeType, storePrecision, storeScale))
  }

  /**
   * Method to add/update Structured Streaming UI Tab if cluster is running in embedded mode or
   * smart connector mode using SnappyData's Spark distribution
   */
  private def updateStructuredStreamingUITab(streams: StreamingQueryManager): Unit = synchronized {
    try {
      val updateUIMethod = classOf[SparkSession].getMethod("updateUIWithStructuredStreamingTab")
      val listener = updateUIMethod.invoke(this, streams).asInstanceOf[StreamingQueryListener]
      val finalizer = this.finalizer
      if (finalizer ne null) {
        finalizer.registerStreamingListener(streams, listener)
      }
    } catch {
      case _: NoSuchMethodException =>
        logInfo("Unable to add Structured Streaming UI Tab because " +
            "updateUIWithStructuredStreamingTab method is not present in SparkSession class. " +
            "It seems spark distribution used is not snappy-spark distribution.")
      case e: InvocationTargetException => throw e.getTargetException
    }
  }
}

/**
 * Trait that adds cloneSession() added in new Spark releases but absent in older
 * ones. SnappySession can override this cleanly and be source compatible with both.
 */
trait SnappySessionLike {

  /** used by [[cloneSession()]] to copy the SessionState */
  private[sql] var cloneSessionState: Option[SnappySessionState] = None

  private[sql] def cloneSession(): SparkSession
}

private class FinalizeSession(session: SnappySession)
    extends FinalizeObject(session, true) {

  private var sessionId = session.id

  private var listenerBus: Option[StreamingQueryListenerBus] = None
  private var listener: Option[StreamingQueryListener] = None

  // Doing this clean up in finalizer as lifecycle of the listener is aligned with session's
  // lifecycle. After this the listener object will be eligible for GC in the next cycle.
  // Also the memory footprint of the listener object is not much hence it should be ok if the
  // listener object is remain alive for one extra GC cycle as compared to the session.
  def registerStreamingListener(streamingManager: StreamingQueryManager,
      listener: StreamingQueryListener): Unit = {
    this.listenerBus = None
    this.listener = None
    val m = SnappySession.listenerBusMethod
    if (m ne null) {
      val l = m.invoke(streamingManager).asInstanceOf[StreamingQueryListenerBus]
      if (l ne null) {
        this.listenerBus = Some(l)
        this.listener = Some(listener)
      }
    }
  }

  override def getHolder: FinalizeHolder = FinalizeObject.getServerHolder

  override def doFinalize(): Boolean = {
    if (sessionId != SnappySession.INVALID_ID) {
      SnappySession.clearSessionCache(sessionId)
      sessionId = SnappySession.INVALID_ID
    }
    listenerBus match {
      case Some(bus) => listener match {
        case Some(l) => bus.removeListener(l)
        case _ =>
      }
      case _ =>
    }
    listenerBus = None
    listener = None
    true
  }

  override protected def clearThis(): Unit = {
    sessionId = SnappySession.INVALID_ID
    listenerBus = None
    listener = None
  }
}

object SnappySession extends SparkSupport with Logging {

  private[spark] val INVALID_ID = -1L
  private[this] val ID = new AtomicLong(0L)
  private[sql] val ExecutionKey = "EXECUTION"
  private[sql] val PUTINTO_LOCK = "putinto_lock"
  private[sql] val CACHED_PUTINTO_LOGICAL_PLAN = "cached_putinto_logical_plan"
  private[sql] val BULKWRITE_LOCK = "bulkwrite_lock"
  private[sql] val WRITE_LOCK_PREFIX = "BULKWRITE_"
  private[sql] val EMPTY_PARAMS = Array.empty[ParamLiteral]
  private[sql] val EMPTY_SHUFFLE_CLEANUPS = Array.empty[Future[Unit]]

  private val unresolvedStarRegex =
    """(cannot resolve ')(\w+).(\w+).*(' given input columns.*)""".r
  private val unresolvedColRegex =
    """(cannot resolve '`)(\w+).(\w+).(\w+)(.*given input columns.*)""".r

  private[sql] lazy val (listenerBusMethod, listenerTag) = {
    try {
      val m = classOf[StreamingQueryManager].getDeclaredMethod("listenerBus")
      m.setAccessible(true)
      (m, ClassTag[Any](org.apache.spark.util.Utils.classForName(
        "org.apache.spark.sql.streaming.SnappyStreamingQueryListener")))
    } catch {
      case _: Exception => (null, null)
    }
  }

  private[sql] def isHiveSupportEnabled(v: String): Boolean = Utils.toLowerCase(v) match {
    case "hive" => true
    case "in-memory" => false
    case _ => throw new IllegalArgumentException(
      s"Unexpected value '$v' for ${StaticSQLConf.CATALOG_IMPLEMENTATION.key}. " +
          "Allowed values are: in-memory and hive")
  }

  def tableIdentifier(table: String, catalog: SnappySessionCatalog,
      resolve: Boolean): TableIdentifier = {
    // hive meta-store is case-insensitive so use lower case names for object names consistently
    val fullName =
      if (catalog ne null) catalog.formatTableName(table) else JdbcExtendedUtils.toLowerCase(table)
    val dotIndex = fullName.indexOf('.')
    if (dotIndex > 0) {
      new TableIdentifier(fullName.substring(dotIndex + 1),
        Some(fullName.substring(0, dotIndex)))
    } else if (resolve && (catalog ne null)) {
      new TableIdentifier(fullName, Some(catalog.getCurrentDatabase))
    } else new TableIdentifier(fullName, None)
  }

  private[sql] def findShuffleDependencies(rdd: RDD[_]): List[Int] = {
    rdd.dependencies.toList.flatMap {
      case s: ShuffleDependency[_, _, _] => if (s.rdd ne rdd) {
        s.shuffleId :: findShuffleDependencies(s.rdd)
      } else s.shuffleId :: Nil

      case d => if (d.rdd ne rdd) findShuffleDependencies(d.rdd) else Nil
    }
  }

  private[sql] def cleanupBroadcasts(plan: SparkPlan, blocking: Boolean): Unit = {
    plan.sqlContext.sparkContext.cleaner match {
      case Some(cleaner) => plan.foreach {
        case broadcast: BroadcastExchangeExec =>
          cleaner.doCleanupBroadcast(broadcast.executeBroadcast().id, blocking)
        case _ =>
      }
      case None =>
    }
  }

  def getExecutedPlan(plan: SparkPlan): (SparkPlan, CodegenSparkFallback) = plan match {
    case cg@CodegenSparkFallback(WholeStageCodegenExec(p), _) => (p, cg)
    case cg@CodegenSparkFallback(p, _) => (p, cg)
    case WholeStageCodegenExec(p) => (p, null)
    case _ => (plan, null)
  }

  private[sql] def setExecutionProperties(localProperties: Properties, executionId: String,
      jobGroupId: String, queryLongForm: String): Unit = {
    localProperties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
    // trim query string to 10K to keep its UTF8 form always < 32K which is the limit
    // for DataOutput.writeUTF used during task serialization
    localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION,
      CachedDataFrame.queryStringShortForm(queryLongForm, 10240))
    localProperties.setProperty(SparkContext.SPARK_JOB_GROUP_ID, jobGroupId)
  }

  private[sql] def clearExecutionProperties(localProperties: Properties,
      oldExecutionId: String): Unit = {
    localProperties.remove(SparkContext.SPARK_JOB_GROUP_ID)
    localProperties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
    if (oldExecutionId eq null) localProperties.remove(SQLExecution.EXECUTION_ID_KEY)
    else localProperties.setProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
  }

  private[sql] def isCommandExec(plan: SparkPlan): Boolean = plan match {
    case _: ExecutedCommandExec | _: ExecutePlan | UnionCommands(_) => true
    case _ => false
  }

  /**
   * Snappy's execution happens in two phases. First phase the plan is executed
   * to create a rdd which is then used to create a CachedDataFrame.
   * In second phase, the CachedDataFrame is then used for further actions.
   * For accumulating the metrics for first phase, SparkListenerSQLPlanExecutionStart
   * is fired. This adds the query to the active executions like normal executions but
   * notes it for future full execution if required. This ensures that query is shown
   * in the UI and  new jobs that are run while the plan is being executed are tracked
   * against this executionID. In the second phase, when the query is
   * actually executed, SparkListenerSQLExecutionStart updates the execution
   * data in the active executions from existing one. SparkListenerSQLExecutionEnd is
   * then sent with the accumulated time of both the phases.
   */
  private def planExecution(qe: QueryExecution, session: SnappySession, sql: String,
      paramLiterals: Array[ParamLiteral], paramsId: Int, cachedPlan: Boolean)
      (f: => RDD[InternalRow]): (RDD[InternalRow], String, SparkPlanInfo,
      String, SparkPlanInfo, Long, Long) = {
    var sqlText: String = null
    val executionId = Utils.nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
    val context = session.sparkContext
    var localProperties: Properties = null
    var oldExecutionId: String = null
    var startTime = -1L
    var endTime = -1L
    if (cachedPlan) {
      sqlText = "PLAN [" + sql + ']'
      localProperties = context.getLocalProperties
      startTime = System.currentTimeMillis()
      oldExecutionId = localProperties.getProperty(SQLExecution.EXECUTION_ID_KEY)
      val executionIdStr = java.lang.Long.toString(executionId)
      val jobGroupId = session.snappySessionState.jdbcQueryJobGroupId(executionIdStr)
      setExecutionProperties(localProperties, executionIdStr, jobGroupId, sqlText)
    }
    var success = false
    try {
      // get below two with original "ParamLiteral(" tokens that will be replaced
      // by actual values before every execution
      var queryExecutionStr = qe.toString
      // post with proper values in event which will show up in GUI
      val postQueryExecutionStr = replaceParamLiterals(queryExecutionStr, paramLiterals, paramsId)
      var queryPlanInfo: SparkPlanInfo = null
      var postQueryPlanInfo: SparkPlanInfo = null
      var rdd: RDD[InternalRow] = null
      if (cachedPlan) {
        queryPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(qe.executedPlan)
        postQueryPlanInfo = PartitionedPhysicalScan.updatePlanInfo(queryPlanInfo,
          paramLiterals, paramsId)
        context.listenerBus.post(SparkListenerSQLPlanExecutionStart(
          executionId, CachedDataFrame.queryStringShortForm(sqlText),
          sqlText, postQueryExecutionStr, postQueryPlanInfo, startTime))
        rdd = f
        success = true
        endTime = System.currentTimeMillis()
      } else {
        queryExecutionStr = postQueryExecutionStr
        queryPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(qe.executedPlan,
          paramLiterals, paramsId)
        postQueryPlanInfo = queryPlanInfo
      }
      (rdd, queryExecutionStr, queryPlanInfo, postQueryExecutionStr, postQueryPlanInfo,
          executionId, endTime - startTime)
    } finally {
      if (cachedPlan) {
        clearExecutionProperties(localProperties, oldExecutionId)
        if (endTime == -1L) endTime = System.currentTimeMillis()
        // post the end of SQL at the end of planning phase; this will be re-posted during
        // execution with the submission time adjusted (by the planning time) in CachedDataFrame
        if (success) {
          context.listenerBus.post(SparkListenerSQLPlanExecutionEnd(executionId, endTime))
        } else {
          // cleanups in case of failure
          SnappySession.cleanupBroadcasts(qe.executedPlan, blocking = true)
          session.snappySessionState.clearExecutionData()
          context.listenerBus.post(SparkListenerSQLExecutionEnd(executionId, endTime))
        }
      }
    }
  }

  private def handleCTAS(tableType: CatalogObjectType.Type): (Boolean, Boolean) = {
    if (CatalogObjectType.isTableBackedByRegion(tableType)) {
      if (tableType == CatalogObjectType.Sample) false -> true else true -> false
    }
    // most CTAS writers (FileFormatWriter, KafkaWriter) post their own plans for insert
    // but it does not include SQL string or the top-level plan for ExecuteCommand
    // so post the GUI plans but evaluate the toRdd outside else it will be nested
    // withNewExecutionId() that will fail
    else true -> true
  }

  private def executeCollect(session: SnappySession, qe: QueryExecution,
      plan: SparkPlan): Array[InternalRow] = {
    CachedDataFrame.withCallback(session, df = null, qe, "executeCollect")(
      CachedDataFrame.timedCallback(_ => plan.executeCollect()))
  }

  private def evaluatePlan(qe: QueryExecution, session: SnappySession, sqlShortText: String,
      sqlText: String, paramLiterals: Array[ParamLiteral], paramsId: Int,
      queryHints: Map[String, String]): CachedDataFrame = {
    val (executedPlan, withFallback) = getExecutedPlan(qe.executedPlan)

    // If this has in-memory caching then don't cache since plan can change
    // dynamically after caching due to unpersist etc. Disable for broadcasts
    // too since the RDDs cache broadcast result which can change in subsequent
    // execution with different ParamLiteral values. Also do not cache
    // if snappy tables are not there since the plans may be dependent on constant
    // literals in push down filters etc
    val cachedPlan = session.planCaching && executedPlan.find {
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec |
           _: BroadcastExchangeExec | _: InMemoryTableScanExec |
           // range partitioning may change on token change hence SortExec is excluded
           _: RangeExec | _: LocalTableScanExec | _: RDDScanExec | _: SortExec => true
      case p if HiveClientUtil.isHiveExecPlan(p) => true
      case _: DataSourceScanExec => true
      case _ => false
    }.isEmpty

    val (cachedRDD, execution, origExecutionString, executionString, origPlanInfo, planInfo,
    cachedRDDId, noSideEffects, executionId, planningTime: Long) = executedPlan match {
      case _ if isCommandExec(executedPlan) =>
        // TODO add caching for point updates/deletes; a bit of complication
        // because getPlan will have to do execution with all waits/cleanups
        // normally done in CachedDataFrame.collectWithHandler/withCallback
        /*
        val cachedRDD = plan match {
          case p: ExecutePlan => p.child.execute()
          case _ => null
        }
        */
        // post with proper values in event which will show up in GUI
        val planInfo = PartitionedPhysicalScan.getSparkPlanInfo(qe.executedPlan,
          paramLiterals, paramsId)
        val executionStr = replaceParamLiterals(qe.toString, paramLiterals, paramsId)
        // don't post separate plan for CTAS since it already has posted one for the insert
        val (eagerCollect, postGUIPlans) = executedPlan.collectFirst {
          case ExecutedCommandExec(c: CreateTableUsingCommand) if c.query.isDefined =>
            handleCTAS(SnappyContext.getProviderType(c.provider))
          case ExecutedCommandExec(c: CreateDataSourceTableAsSelectCommand) =>
            handleCTAS(CatalogObjectType.getTableType(c.table))
          case ExecutedCommandExec(c: SnappyCacheTableCommand) if !c.isLazy => true -> false
          // other commands may have their own withNewExecutionId but still post GUI
          // plans to see the command with proper SQL string in the GUI
          case _: ExecutedCommandExec => true -> true
        } match {
          case None => false -> true
          case Some(p) => p
        }

        val execPlan = qe.executedPlan // include CodegenSparkFallback
        var result = if (eagerCollect) executeCollect(session, qe, execPlan) else null

        // post final execution immediately (collect for these plans will post nothing)
        CachedDataFrame.withNewExecutionId(session, execPlan, sqlShortText, sqlText,
          executionStr, planInfo, postGUIPlans = postGUIPlans) {
          // create new LocalRelation so that plan does not get re-executed
          // (e.g. just toRdd is not enough since further operators like show will pass
          //   around the LogicalPlan and not the executedPlan; it works for plans using
          //   ExecutedCommandExec though because Spark layer has special check for it in
          //   Dataset.logicalPlan)
          if (result eq null) result = executeCollect(session, qe, execPlan)
          val newPlan = LocalRelation(execPlan.output, result)
          val execution = session.sessionState.executePlan(newPlan)
          (null, execution, executionStr, executionStr, planInfo, planInfo,
              -1, false, -1L, 0L)
        }._1

      case plan: CollectAggregateExec =>
        val (rdd, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId, planTime) =
          planExecution(qe, session, sqlText, paramLiterals, paramsId, cachedPlan)(
            if (withFallback ne null) withFallback.execute(plan.child) else plan.childRDD)
        (rdd, qe, origExecutionStr, executionStr, origPlanInfo, planInfo,
            -1, true, executionId, planTime)

      case p =>
        val (rdd, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId, planTime) =
          planExecution(qe, session, sqlText, paramLiterals, paramsId, cachedPlan)(p match {
            case p: CollectLimitExec =>
              if (withFallback ne null) withFallback.execute(p.child) else p.child.execute()
            case _ => qe.executedPlan.execute()
          })
        (rdd, qe, origExecutionStr, executionStr, origPlanInfo, planInfo,
            -1, true, executionId, planTime)
    }

    logDebug(s"qe.executedPlan = ${qe.executedPlan}")

    val checkAuthOfExternalTables = java.lang.Boolean.parseBoolean(
      System.getProperty("CHECK_EXTERNAL_TABLE_AUTHZ"))
    // second condition means smart connector mode
    if (checkAuthOfExternalTables || (ToolsCallbackInit.toolsCallback == null)) {
      // check for external tables in the plan.
      val scanNodes = executedPlan.collect {
        case dsc: DataSourceScanExec if !dsc.relation.isInstanceOf[PartitionedDataSourceScan] => dsc
      }
      if (scanNodes.nonEmpty) {
        checkCurrentUserAllowed(session, scanNodes)
      }
    }

    val (rdd, rddId, shuffleDependencies, shuffleCleanups) = if (cachedPlan && cachedRDD != null) {
      val shuffleDeps = findShuffleDependencies(cachedRDD).toArray
      val cleanups = new Array[Future[Unit]](shuffleDeps.length)
      (cachedRDD, cachedRDD.id, shuffleDeps, cleanups)
    } else {
      (null, if (cachedRDDId != -1) cachedRDDId else session.newRddId(),
          Array.emptyIntArray, EMPTY_SHUFFLE_CLEANUPS)
    }
    new CachedDataFrame(session, execution, origExecutionString, executionString, origPlanInfo,
      planInfo, rdd, shuffleDependencies, RowEncoder(qe.analyzed.schema), CachedDataFrame.wrap(
        paramLiterals), paramsId, sqlShortText, sqlText, shuffleCleanups, rddId, noSideEffects,
      queryHints, executionId, planningTime, session.hasLinkPartitionsToBuckets)
  }

  private def checkCurrentUserAllowed(session: SnappySession,
      scanNodes: Seq[DataSourceScanExec]): Unit = {
    var currentUser = SnappyContext.getClusterMode(session.sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
        session.conf.get(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR, null)
      case _ => session.conf.get(Attribute.USERNAME_ATTR, default = null)
    }
    if (currentUser eq null) currentUser = ""
    if (ToolsCallbackInit.toolsCallback != null) {
      scanNodes.foreach { scan =>
        val authzException = ToolsCallbackInit.toolsCallback.isUserAuthorizedForExtTable(
          currentUser, internals.tableIdentifier(scan))
        if (authzException ne null) throw authzException
      }
    } else if (SnappyContext.getClusterMode(session.sparkContext)
        .isInstanceOf[ThinClientConnectorMode]) {
      // Smart Connector Mode
      val tables = scanNodes.collect {
        case scan if internals.tableIdentifier(scan).isDefined =>
          internals.tableIdentifier(scan).get.unquotedString
      }
      if (tables.nonEmpty) {
        val connection = session.getConnection(tables.head)
        val cstmt = connection.prepareCall("call sys.CHECK_AUTHZ_ON_EXT_TABLES(?, ?, ?)")
        try {
          cstmt.setString(1, currentUser)
          val allTablesStr = tables.mkString(",")
          cstmt.setString(2, allTablesStr)
          cstmt.registerOutParameter(3, java.sql.Types.VARCHAR)
          cstmt.execute()
          val ret = cstmt.getString(3)
          if (ret != null && ret.nonEmpty) {
            // throw exception
            throw new AnalysisException(s"$currentUser  not authorized to access $ret")
          }
        } finally {
          if (cstmt != null) cstmt.close()
          if (connection != null) connection.close()
        }
      }
    }
  }

  private[this] lazy val planCache = {
    val env = SparkEnv.get
    val cacheSize = if (env ne null) {
      Property.PlanCacheSize.get(env.conf)
    } else Property.PlanCacheSize.defaultValue.get
    CacheBuilder.newBuilder().maximumSize(cacheSize).build[CachedKey, CachedDataFrame]()
  }

  // noinspection UnstableApiUsage
  def getPlanCache: Cache[CachedKey, CachedDataFrame] = planCache

  def sqlPlan(session: SnappySession, sqlText: String): CachedDataFrame = {
    val parser = session.snappySessionState.snappySqlParser
    val sqlShortText = CachedDataFrame.queryStringShortForm(sqlText)
    val plan = parser.parsePlan(sqlText, clearExecutionData = true)
    val planCaching = session.planCaching
    val sqlParser = parser.sqlParser
    val paramLiterals = sqlParser.getAllLiterals
    val paramsId = sqlParser.getCurrentParamsId
    val queryHints = session.getLastQueryHints
    val key = CachedKey(session, session.getCurrentSchema,
      plan, sqlText, paramLiterals, paramsId, planCaching, queryHints.hashCode())
    var cachedDF: CachedDataFrame = if (planCaching) planCache.getIfPresent(key) else null
    if (cachedDF eq null) {
      // evaluate the plan and cache it if required
      session.currentKey = key
      try {
        val execution = session.executePlan(plan)
        cachedDF = evaluatePlan(execution, session, sqlShortText, sqlText,
          paramLiterals, paramsId, queryHints)
        // put in cache if the DF has to be cached
        if (planCaching && cachedDF.isCached) {
          if (isTraceEnabled) {
            logTrace(s"Caching the plan for: $sqlText :: ${cachedDF.queryExecutionString}")
          } else if (isDebugEnabled) {
            logDebug(s"Caching the plan for: $sqlText")
          }
          planCache.put(key, cachedDF)
        }
      } finally {
        session.currentKey = null
      }
    } else {
      logDebug(s"Using cached plan for: $sqlText (existing: ${cachedDF.queryString})")
      cachedDF = cachedDF.withCurrentLiterals(paramLiterals, sqlShortText, sqlText)
    }
    cachedDF
  }

  /**
   * Replace any ParamLiterals in a string with current values.
   */
  private[sql] def replaceParamLiterals(text: String,
      currentParamConstants: Array[ParamLiteral], paramsId: Int): String = {

    if ((currentParamConstants eq null) || currentParamConstants.length == 0) return text
    val paramStart = TokenLiteral.PARAMLITERAL_START
    var nextIndex = text.indexOf(paramStart)
    if (nextIndex != -1) {
      var lastIndex = 0
      val sb = new java.lang.StringBuilder(text.length)
      while (nextIndex != -1) {
        sb.append(text, lastIndex, nextIndex)
        nextIndex += paramStart.length
        val posEnd = text.indexOf(',', nextIndex)
        val pos = Integer.parseInt(text.substring(nextIndex, posEnd))
        // get the ID which created this ParamLiteral (e.g. a query on temporary table
        // for a previously cached table will have its own literals and cannot replace former)
        val idEnd = text.indexOf('#', posEnd + 1)
        val id = Integer.parseInt(text.substring(posEnd + 1, idEnd))
        val lenEnd = text.indexOf(',', idEnd + 1)
        val len = Integer.parseInt(text.substring(idEnd + 1, lenEnd))
        lastIndex = lenEnd + 1 + len
        // append the new value if matching ID else use the value embedded in `text`
        if (paramsId == id) {
          sb.append(currentParamConstants(pos).valueString)
          // skip to end of value and continue searching
        } else {
          sb.append(text.substring(lenEnd + 1, lastIndex))
        }
        nextIndex = text.indexOf(paramStart, lastIndex)
      }
      // append any remaining
      if (lastIndex < text.length) {
        sb.append(text, lastIndex, text.length)
      }
      sb.toString
    } else text
  }

  private def newId(): Long = {
    val id = ID.incrementAndGet()
    if (id != INVALID_ID) id else ID.incrementAndGet()
  }

  private[spark] def clearSessionCache(sessionId: Long): Unit = {
    val iter = planCache.asMap().keySet().iterator()
    while (iter.hasNext) {
      val session = iter.next().session
      if (session.id == sessionId) {
        iter.remove()
      }
    }
  }

  def clearAllCache(onlyQueryPlanCache: Boolean = false): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (!SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION &&
        (sc ne null) && !sc.isStopped) {
      planCache.invalidateAll()
      if (!onlyQueryPlanCache) {
        RefreshMetadata.executeOnAll(sc, RefreshMetadata.CLEAR_CODEGEN_CACHE, args = null)
      }
    }
  }

  // One-to-One Mapping with SparkSQLPrepareImpl.getSQLType
  def getDataType(storeType: Int, precision: Int, scale: Int): DataType = storeType match {
    case StoredFormatIds.SQL_INTEGER_ID => IntegerType
    case StoredFormatIds.SQL_CLOB_ID => StringType
    case StoredFormatIds.SQL_LONGINT_ID => LongType
    case StoredFormatIds.SQL_TIMESTAMP_ID => TimestampType
    case StoredFormatIds.SQL_DATE_ID => DateType
    case StoredFormatIds.SQL_DOUBLE_ID => DoubleType
    case StoredFormatIds.SQL_DECIMAL_ID =>
      if (precision == -1) DecimalType.SYSTEM_DEFAULT
      else if (precision == DecimalType.SYSTEM_DEFAULT.precision &&
          scale == DecimalType.SYSTEM_DEFAULT.scale) {
        DecimalType.SYSTEM_DEFAULT
      }
      else if (precision == DecimalType.USER_DEFAULT.precision &&
          scale == DecimalType.USER_DEFAULT.scale) {
        DecimalType.USER_DEFAULT
      }
      else {
        assert(precision >= 0)
        assert(scale >= 0)
        DecimalType(precision, scale)
      }
    case StoredFormatIds.SQL_REAL_ID => FloatType
    case StoredFormatIds.SQL_BOOLEAN_ID => BooleanType
    case StoredFormatIds.SQL_SMALLINT_ID => ShortType
    case StoredFormatIds.SQL_TINYINT_ID => ByteType
    case StoredFormatIds.SQL_BLOB_ID => BinaryType
    case _ => StringType
  }

  def getValue(dvd: stypes.DataValueDescriptor,
      returnUTF8String: Boolean = true): Any = dvd match {
    case i: stypes.SQLInteger => i.getInt
    case si: stypes.SQLSmallint => si.getShort
    case ti: stypes.SQLTinyint => ti.getByte
    case d: stypes.SQLDouble => d.getDouble
    case li: stypes.SQLLongint => li.getLong
    case bid: stypes.BigIntegerDecimal => bid.getDouble
    case de: stypes.SQLDecimal => de.getBigDecimal
    case r: stypes.SQLReal => r.getFloat
    case b: stypes.SQLBoolean => b.getBoolean
    case cl: stypes.SQLClob =>
      val charArray = cl.getCharArray()
      if (charArray != null) {
        val str = String.valueOf(charArray)
        if (returnUTF8String) UTF8String.fromString(str) else str
      } else null
    case lvc: stypes.SQLLongvarchar =>
      if (returnUTF8String) UTF8String.fromString(lvc.getString) else lvc.getString
    case vc: stypes.SQLVarchar =>
      if (returnUTF8String) UTF8String.fromString(vc.getString) else vc.getString
    case c: stypes.SQLChar =>
      if (returnUTF8String) UTF8String.fromString(c.getString) else c.getString
    case ts: stypes.SQLTimestamp => ts.getTimestamp(null)
    case t: stypes.SQLTime => t.getTime(null)
    case d: stypes.SQLDate =>
      val c: Calendar = null
      d.getDate(c)
    case _ => dvd.getObject
  }

  var jarServerFiles: Array[String] = Array.empty

  def getJarURIs: Array[String] = {
    SnappySession.synchronized({
      jarServerFiles
    })
  }

  def addJarURIs(uris: Array[String]): Unit = {
    SnappySession.synchronized({
      jarServerFiles = jarServerFiles ++ uris
    })
  }
}

final class CachedKey(val session: SnappySession,
    val currSchema: String, private val lp: LogicalPlan,
    val sqlText: String, val hintHashCode: Int,
    val paramLiterals: Array[ParamLiteral], val paramsId: Int) {

  override val hashCode: Int = {
    var h = ClientResolverUtils.addIntToHashOpt(session.hashCode(), 42)
    h = ClientResolverUtils.addIntToHashOpt(currSchema.hashCode, h)
    h = ClientResolverUtils.addIntToHashOpt(lp.hashCode(), h)
    ClientResolverUtils.addIntToHashOpt(hintHashCode, h)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: CachedKey =>
        (x eq this) || (x.hintHashCode == hintHashCode && x.hashCode == hashCode &&
            (x.session eq session) && x.currSchema == currSchema && x.lp.fastEquals(lp))
      case _ => false
    }
  }
}

object CachedKey extends SparkSupport {

  def apply(session: SnappySession, currschema: String, plan: LogicalPlan, sqlText: String,
      paramLiterals: Array[ParamLiteral], paramsId: Int, forCaching: Boolean,
      hintHashCode: Int): CachedKey = {

    def normalizeExprIds: PartialFunction[Expression, Expression] = {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(-1))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(-1))
      case ae: AggregateExpression => ae.copy(resultId = ExprId(-1))
      case _: ScalarSubquery =>
        throw new IllegalStateException("scalar subquery should not have been present")
      case e: Exists =>
        e.copy(plan = e.plan.transformAllExpressions(normalizeExprIds), exprId = ExprId(-1))
      case p if internals.isPredicateSubquery(p) =>
        internals.copyPredicateSubquery(p, p.asInstanceOf[PlanExpression[LogicalPlan]].plan
            .transformAllExpressions(normalizeExprIds), ExprId(-1))
      case l: ListQuery =>
        l.copy(plan = l.plan.transformAllExpressions(normalizeExprIds), exprId = ExprId(-1))
        // create a tokenized ParamLiteral to compare by position for caching
      case pl: ParamLiteral if !pl.isTokenized => pl.copy(tokenized = true)
    }

    def transformExprIDs: PartialFunction[LogicalPlan, LogicalPlan] = {
      case f@Filter(condition, child) => f.copy(
        condition = condition.transform(normalizeExprIds),
        child = child.transformAllExpressions(normalizeExprIds))
      case q: LogicalPlan => q.transformAllExpressions(normalizeExprIds)
    }

    // normalize lp so that two queries can be determined to be equal
    val normalizedPlan = if (forCaching) plan.transform(transformExprIDs) else plan
    new CachedKey(session, currschema, normalizedPlan, sqlText, hintHashCode,
      paramLiterals, paramsId)
  }
}

/**
 * A new event that is fired when a plan is executed to get an RDD.
 */
case class SparkListenerSQLPlanExecutionStart(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    sparkPlanInfo: SparkPlanInfo,
    time: Long)
    extends SparkListenerEvent

case class SparkListenerSQLPlanExecutionEnd(executionId: Long, time: Long)
    extends SparkListenerEvent

private object UnionCommands {
  def unapply(plan: SparkPlan): Option[Boolean] = plan match {
    case union: UnionExec if union.children.nonEmpty && union.children.forall {
      case _: ExecutedCommandExec | _: ExecutePlan => true
      case _ => false
    } => Some(true)
    case _ => None
  }
}
