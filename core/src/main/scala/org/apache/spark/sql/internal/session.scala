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

import java.util.Properties
import java.util.function.BiConsumer

import scala.reflect.{ClassTag, classTag}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.{Attribute => GAttr}
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, TypedConfigBuilder}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute, UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Contains, EndsWith, EqualTo, Expression, Like, Literal, NamedExpression, StartsWith, TokenLiteral}
import org.apache.spark.sql.catalyst.optimizer.ReorderJoin
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project, UnaryNode, Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation, PreprocessTableInsertion}
import org.apache.spark.sql.execution.{SecurityUtils, SparkOptimizer}
import org.apache.spark.sql.hive.SnappySessionState
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DecimalType, LongType, StringType}
import org.apache.spark.sql.{AnalysisException, DMLExternalTable, SaveMode, SnappyContext, SnappyParser, SnappySession, SparkSupport}
import org.apache.spark.unsafe.types.UTF8String

// Misc helper classes for session handling

class SnappyConf(@transient val session: SnappySession)
    extends SQLConf with Serializable {

  /** Pool to be used for the execution of queries from this session */
  @volatile private[this] var schedulerPool: String = Property.SchedulerPool.defaultValue.get

  /** If shuffle partitions is set by [[setExecutionShufflePartitions]]. */
  @volatile private[this] var executionShufflePartitions: Int = _

  /**
   * Records the number of shuffle partitions to be used determined on runtime
   * from available cores on the system. A value < 0 indicates that it was set
   * explicitly by user and should not use a dynamic value. A value of 0 indicates
   * that current plan execution did not touch it so reset need not touch it.
   */
  @volatile private[this] var dynamicShufflePartitions: Int = _

  /**
   * Set if a task implicitly sets the "spark.task.cpus" property based
   * on executorCores/physicalCores. This will be -1 if set explicitly
   * on the session so that it doesn't get reset at the end of task execution.
   */
  @volatile private[this] var dynamicCpusPerTask: Int = _

  SQLConf.SHUFFLE_PARTITIONS.defaultValue match {
    case Some(d) if (session ne null) && super.numShufflePartitions == d =>
      dynamicShufflePartitions = coreCountForShuffle
    case None if session ne null =>
      dynamicShufflePartitions = coreCountForShuffle
    case _ =>
      executionShufflePartitions = -1
      dynamicShufflePartitions = -1
  }

  resetOverrides()

  private def resetOverrides(): Unit = {
    val overrideConfs = session.overrideConfs
    if (overrideConfs.nonEmpty) {
      overrideConfs.foreach(p => setConfString(p._1, p._2))
    }
  }

  private def coreCountForShuffle: Int = {
    val count = SnappyContext.totalPhysicalCoreCount.get()
    if (count > 0 || (session eq null)) math.min(super.numShufflePartitions, count)
    else math.min(super.numShufflePartitions, session.sparkContext.defaultParallelism)
  }

  private lazy val allDefinedKeys = {
    val map = new CaseInsensitiveMutableHashMap[String](Map.empty)
    getAllDefinedConfs.foreach(e => map.put(e._1, e._1))
    map
  }

  private def keyUpdateActions(key: String, value: Option[Any],
      doSet: Boolean, search: Boolean = true): String = key match {
    // clear plan cache when some size related key that effects plans changes
    case SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key |
         Property.HashJoinSize.name |
         Property.HashAggregateSize.name |
         Property.ForceLinkPartitionsToBuckets.name |
         Property.TestCodeSplitFunctionParamsSizeInSHA.name |
         Property.TestCodeSplitThresholdInSHA.name |
         Property.UseOptimzedHashAggregate.name |
         Property.UseOptimizedHashAggregateForSingleKey.name |
         Property.TestExplodeComplexDataTypeInSHA.name =>
      session.clearPlanCache()
      key

    case SQLConf.SHUFFLE_PARTITIONS.key =>
      // stop dynamic determination of shuffle partitions
      if (doSet) {
        executionShufflePartitions = -1
        dynamicShufflePartitions = -1
      } else {
        dynamicShufflePartitions = coreCountForShuffle
      }
      session.clearPlanCache()
      key

    case Property.SchedulerPool.name =>
      schedulerPool = value match {
        case None => Property.SchedulerPool.defaultValue.get
        case Some(pool: String) if session.sparkContext.getPoolForName(pool).isDefined => pool
        case Some(pool) => throw new IllegalArgumentException(s"Invalid Pool $pool")
      }
      key

    case Property.PartitionPruning.name =>
      value match {
        case Some(b) => session.partitionPruning = b.toString.toBoolean
        case None => session.partitionPruning = Property.PartitionPruning.defaultValue.get
      }
      session.clearPlanCache()
      key

    case Property.PlanCaching.name =>
      val b = value match {
        case Some(boolVal) => boolVal.toString.toBoolean
        case None => Property.PlanCaching.defaultValue.get
      }
      if (!b && session.planCaching) session.clearPlanCache()
      session.planCaching = b
      key

    case Property.DisableHashJoin.name =>
      value match {
        case Some(boolVal) => session.disableHashJoin = boolVal.toString.toBoolean
        case None => session.disableHashJoin = Property.DisableHashJoin.defaultValue.get
      }
      session.clearPlanCache()
      key

    case CATALOG_IMPLEMENTATION.key if !session.hiveInitializing =>
      val newValue = value match {
        case Some(v) => SnappySession.isHiveSupportEnabled(v.toString)
        case None => CATALOG_IMPLEMENTATION.defaultValueString == "hive"
      }
      // initialize hive session upfront
      if (newValue) {
        session.hiveInitializing = true
        assert(session.snappySessionState.hiveSession ne null)
        session.hiveInitializing = false
      }
      session.enableHiveSupport = newValue
      key

    case Property.Compatibility.name =>
      value match {
        case Some(level) => Utils.toLowerCase(level.toString) match {
          case "snappy" | "spark" | "hive" =>
          case _ => throw new IllegalArgumentException(
            s"Unexpected value '$level' for ${Property.Compatibility.name}. " +
                "Allowed values are: snappy, spark and hive")
        }
        case None =>
      }
      key

    case SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key =>
      session.clearPlanCache()
      key

    case Constant.TRIGGER_AUTHENTICATION => value match {
      case Some(boolVal) if boolVal.toString.toBoolean =>
        if ((Misc.getMemStoreBootingNoThrow ne null) && Misc.isSecurityEnabled) {
          SecurityUtils.checkCredentials(getConfString(
            com.pivotal.gemfirexd.Attribute.USERNAME_ATTR),
            getConfString(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)) match {
            case None => // success
            case Some(failure) =>
              throw Util.generateCsSQLException(SQLState.NET_CONNECT_AUTH_FAILED, failure)
          }
        }
        key
      case _ => key
    }

    case Constant.CPUS_PER_TASK_PROP => value match {
      case Some(intVal) =>
        val intStr = intVal.toString
        val numCpus = intStr.toInt
        if (numCpus < 1) {
          throw new IllegalArgumentException(s"Value for ${Constant.CPUS_PER_TASK_PROP} " +
              s"should be >= 1 but was $intVal")
        }
        session.sparkContext.setLocalProperty(Constant.CPUS_PER_TASK_PROP, intStr)
        dynamicCpusPerTask = -1
        key
      case _ =>
        session.sparkContext.setLocalProperty(Constant.CPUS_PER_TASK_PROP, null)
        dynamicCpusPerTask = 0
        key
    }

    case GAttr.USERNAME_ATTR | GAttr.USERNAME_ALT_ATTR | GAttr.PASSWORD_ATTR => key

    case _ if key.startsWith("spark.sql.aqp.") =>
      session.clearPlanCache()
      key

    case _ =>
      // search case-insensitively for other keys if required
      if (search) {
        allDefinedKeys.get(key) match {
          case None => key
          case Some(k) =>
            // execute keyUpdateActions again since it might be one of the pre-defined ones
            keyUpdateActions(k, value, doSet, search = false)
            k
        }
      } else key
  }

  private def hiveConf: SQLConf = session.snappySessionState.hiveSession.sessionState.conf

  private[sql] def resetDefaults(): Unit = synchronized {
    if (session ne null) {
      if (executionShufflePartitions != -1) {
        executionShufflePartitions = 0
      }
      if (dynamicShufflePartitions != -1) {
        dynamicShufflePartitions = coreCountForShuffle
      }
      if (dynamicCpusPerTask > 0) {
        unsetConf(Constant.CPUS_PER_TASK_PROP)
      }
    }
  }

  private[sql] def setExecutionShufflePartitions(n: Int): Unit = synchronized {
    if (executionShufflePartitions != -1 && session != null) {
      executionShufflePartitions = math.max(n, executionShufflePartitions)
      logDebug(s"Set execution shuffle partitions to $executionShufflePartitions")
    }
  }

  private[sql] def setDynamicCpusPerTask(): Unit = synchronized {
    if (dynamicCpusPerTask != -1) {
      val numExecutors = SnappyContext.numExecutors
      val physicalCores = SnappyContext.totalPhysicalCoreCount.get()
      val totalUsableHeap = SnappyContext.foldLeftBlockIds(0L)(_ + _.usableHeapBytes)

      // skip for smart connector where there is no information of physical cores or heap
      if (numExecutors == 0 || physicalCores <= 0 || totalUsableHeap <= 0) return

      val sparkCores = session.sparkContext.defaultParallelism.toDouble
      // calculate minimum required heap assuming a block size of 128M
      val minRequiredHeap = 128.0 * 1024.0 * 1024.0 * sparkCores * 1.2

      // select bigger among (required heap / available) and (logical cores / physical)
      val cpusPerTask0 = math.max(minRequiredHeap / totalUsableHeap, sparkCores / physicalCores)
      // keep a reasonable upper-limit so tasks can at least be scheduled:
      //   used below is average logical cores / 2
      val cpusPerTask = math.min(physicalCores, math.max(1, math.ceil(math.min(sparkCores /
          (2 * numExecutors), cpusPerTask0)).toInt))
      setConfString(Constant.CPUS_PER_TASK_PROP, cpusPerTask.toString)
      dynamicCpusPerTask = cpusPerTask
      logDebug(s"Set dynamic ${Constant.CPUS_PER_TASK_PROP} to $cpusPerTask")
    }
  }

  override def numShufflePartitions: Int = {
    val partitions = this.executionShufflePartitions
    if (partitions > 0) partitions
    else {
      val partitions = this.dynamicShufflePartitions
      if (partitions > 0) partitions else super.numShufflePartitions
    }
  }

  def activeSchedulerPool: String = schedulerPool

  override def setConfString(key: String, value: String): Unit = {
    val rkey = keyUpdateActions(key, Some(value), doSet = true)
    super.setConfString(rkey, value)
    if (session.enableHiveSupport) hiveConf.setConfString(rkey, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    keyUpdateActions(entry.key, Some(value), doSet = true, search = false)
    super.setConf(entry, value)
    if (session.enableHiveSupport) hiveConf.setConf(entry, value)
  }

  override def setConf(props: Properties): Unit = {
    super.setConf(props)
    if (session.enableHiveSupport) hiveConf.setConf(props)
  }

  override def unsetConf(key: String): Unit = {
    val rkey = keyUpdateActions(key, None, doSet = false)
    super.unsetConf(rkey)
    if (session.enableHiveSupport) hiveConf.unsetConf(rkey)
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    keyUpdateActions(entry.key, None, doSet = false, search = false)
    super.unsetConf(entry)
    if (session.enableHiveSupport) hiveConf.unsetConf(entry)
  }

  def foreach(f: (String, String) => Unit): Unit = settings.synchronized {
    settings.forEach(new BiConsumer[String, String] {
      override def accept(k: String, v: String): Unit = f(k, v)
    })
  }

  override def clear(): Unit = {
    super.clear()
    resetOverrides()
  }

  override def clone(): SQLConf = {
    val result = new SnappyConf(session)
    getAllConfs.foreach {
      case(k, v) => if (v ne null) result.setConfString(k, v)
    }
    result
  }
}

class SQLConfigEntry private(private[sql] val entry: ConfigEntry[_]) {

  def key: String = entry.key

  def doc: String = entry.doc

  def isPublic: Boolean = entry.isPublic

  def defaultValue[T]: Option[T] = entry.defaultValue.asInstanceOf[Option[T]]

  def defaultValueString: String = entry.defaultValueString

  def valueConverter[T]: String => T =
    entry.asInstanceOf[ConfigEntry[T]].valueConverter

  def stringConverter[T]: T => String =
    entry.asInstanceOf[ConfigEntry[T]].stringConverter

  override def toString: String = entry.toString
}

object SQLConfigEntry extends SparkSupport {

  private def withParams(builder: ConfigBuilder, doc: String,
      isPublic: Boolean): ConfigBuilder = {
    if (isPublic) builder.doc(doc) else builder.doc(doc).internal()
  }

  private def handleDefault[T](entry: TypedConfigBuilder[T],
      defaultValue: Option[T]): SQLConfigEntry = defaultValue match {
    case Some(v) => new SQLConfigEntry(entry.createWithDefault(v))
    case None => new SQLConfigEntry(entry.createOptional)
  }

  def sparkConf[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](withParams(ConfigBuilder(key),
        doc, isPublic).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](withParams(ConfigBuilder(key),
        doc, isPublic).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](withParams(ConfigBuilder(key),
        doc, isPublic).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](withParams(ConfigBuilder(key),
        doc, isPublic).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](withParams(ConfigBuilder(key), doc, isPublic).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(s"Unknown type of configuration key: $c")
    }
  }

  def apply[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](withParams(internals.buildConf(key),
        doc, isPublic).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](withParams(internals.buildConf(key),
        doc, isPublic).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](withParams(internals.buildConf(key),
        doc, isPublic).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](withParams(internals.buildConf(key),
        doc, isPublic).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](withParams(internals.buildConf(key), doc, isPublic).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(s"Unknown type of configuration key: $c")
    }
  }
}

trait AltName[T] {

  def name: String

  def altName: String

  def configEntry: SQLConfigEntry

  def defaultValue: Option[T] = configEntry.defaultValue[T]

  def getOption(conf: SparkConf): Option[String] = if (altName == null) {
    conf.getOption(name)
  } else {
    conf.getOption(name) match {
      case s: Some[String] => // check if altName also present and fail if so
        if (conf.contains(altName)) {
          throw new IllegalArgumentException(
            s"Both $name and $altName configured. Only one should be set.")
        } else s
      case None => conf.getOption(altName)
    }
  }

  private def get(conf: SparkConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.get(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.get(name, defaultValue)).get
    }
  }

  def get(conf: SparkConf): T = if (altName == null) {
    get(conf, name, configEntry.defaultValueString)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, name, configEntry.defaultValueString)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def get(properties: Properties): T = {
    val propertyValue = getProperty(properties)
    if (propertyValue ne null) configEntry.valueConverter[T](propertyValue)
    else defaultValue.get
  }

  def getProperty(properties: Properties): String = if (altName == null) {
    properties.getProperty(name)
  } else {
    val v = properties.getProperty(name)
    if (v != null) {
      // check if altName also present and fail if so
      if (properties.getProperty(altName) != null) {
        throw new IllegalArgumentException(
          s"Both $name and $altName specified. Only one should be set.")
      }
      v
    } else properties.getProperty(altName)
  }

  def unapply(key: String): Boolean = name.equals(key) ||
      (altName != null && altName.equals(key))
}

trait SQLAltName[T] extends AltName[T] {

  private def get(conf: SQLConf, entry: SQLConfigEntry): T = {
    entry.defaultValue match {
      case Some(_) => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[T]])
      case None => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[Option[T]]]).get
    }
  }

  private def get(conf: SQLConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.getConfString(name, defaultValue)).get
    }
  }

  def get(conf: SQLConf): T = if (altName == null) {
    get(conf, configEntry)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, configEntry)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def getOption(conf: SQLConf): Option[T] = if (altName == null) {
    if (conf.contains(name)) Some(get(conf, name, "<undefined>"))
    else defaultValue
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) Some(get(conf, name, ""))
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else if (conf.contains(altName)) {
      Some(get(conf, altName, ""))
    } else defaultValue
  }

  def set(conf: SQLConf, value: T, useAltName: Boolean = false): Unit = {
    if (useAltName) {
      conf.setConfString(altName, configEntry.stringConverter(value))
    } else {
      conf.setConf[T](configEntry.entry.asInstanceOf[ConfigEntry[T]], value)
    }
  }

  def remove(conf: SQLConf, useAltName: Boolean = false): Unit = {
    conf.unsetConf(if (useAltName) altName else name)
  }
}

trait DefaultOptimizer extends SparkOptimizer {

  def state: SnappySessionState

  def batchesImpl: Seq[Batch] = {
    implicit val ss: SnappySession = state.snappySession
    var insertedSnappyOpts = 0
    val modified = super.batches.map {
      case batch if batch.name.startsWith("Operator Optimization") =>
        insertedSnappyOpts += 1
        val (left, right) = batch.rules.splitAt(batch.rules.indexOf(ReorderJoin))
        Batch(batch.name, batch.strategy, (left :+ ResolveIndex()) ++ right: _*)
      case b => b
    }

    if (insertedSnappyOpts == 0) {
      throw new AnalysisException("Snappy Optimizations not applied")
    }

    modified :+
        Batch("Streaming SQL Optimizers", Once, state.PushDownWindowLogicalPlan) :+
        Batch("Link buckets to RDD partitions", Once, state.LinkPartitionsToBuckets) :+
        Batch("TokenizedLiteral Folding Optimization", Once, state.TokenizedLiteralFolding) :+
        Batch("Order join conditions ", Once, state.OrderJoinConditions)
  }
}

private[sql] final class PreprocessTable(state: SnappySessionState)
    extends Rule[LogicalPlan] with SparkSupport {

  private def conf: SQLConf = state.conf

  private def resolveProjection(u: UnresolvedTableValuedFunction,
      child: LogicalPlan, op: String): (LogicalPlan, LogicalPlan) = {
    val session = state.snappySession
    if (u.functionArgs.forall(_.isInstanceOf[UnresolvedAttribute])) {
      val relation = session.sessionCatalog.resolveRelation(
        session.tableIdentifier(u.functionName, resolve = true))
      val output = relation.output
      val childOutput = child.output
      if (childOutput.length != u.functionArgs.length) {
        throw new AnalysisException("Query in the INSERT/PUT statement " +
            s"(${childOutput.map(_.name).mkString("; ")}) should generate the same number " +
            s"of columns as the table projection (${u.functionArgs.mkString("; ")})")
      }
      // if all columns are being projected then apply the Projections else
      // check for row tables and pass them through since those may have
      // default values or identity columns
      val projection = new Array[NamedExpression](output.length)
      val resolver = state.analyzer.resolver
      var index = -1
      for (i <- u.functionArgs.indices) {
        val e = u.functionArgs(i)
        relation.resolve(e.asInstanceOf[UnresolvedAttribute].nameParts, resolver) match {
          case Some(attr) if (index = output.indexOf(attr)).isInstanceOf[Unit] && index != -1 =>
            projection(index) = internals.newAlias(childOutput(i), output(index).name, None)
          case None =>
            throw new AnalysisException(s"Could not resolve $e for $op " +
                s"in table ${u.functionName} among (${output.map(_.name).mkString(", ")})")
        }
      }
      val isRowTable = relation match {
        case lr: LogicalRelation if lr.relation.isInstanceOf[JDBCMutableRelation] => true
        case _ => false
      }
      val currentKey = session.currentKey
      var hasNullValueProjection = false
      for (i <- projection.indices) {
        if (projection(i) eq null) {
          hasNullValueProjection = true
          // add NULL of target type
          if (!isRowTable || (currentKey eq null)) {
            val attr = output(i)
            if (!attr.nullable) {
              throw new AnalysisException(
                s"For $op in ${u.functionName}, ${attr.name} not specified but is NOT NULL")
            }
            projection(i) = internals.newAlias(Literal(null, attr.dataType), attr.name, None)
          }
        }
      }
      if (hasNullValueProjection && isRowTable && (currentKey ne null)) {
        // fallback to store-layer SQL to handle possible default and autoincrement columns
        // TODO: handle default (using Metadata query) and autoinc (using builtin functions)
        (u, DMLExternalTable(relation, currentKey.sqlText))
      } else (relation, Project(projection.toSeq, child))
    } else (u, child)
  }

  def apply(plan: LogicalPlan): LogicalPlan = internals.logicalPlanResolveDown(plan) {

    // Add dbtable property for create table. While other routes can add it in
    // SnappySession.createTable, the DataFrameWriter path needs to be handled here.
    case c@CreateTable(tableDesc, mode, queryOpt) if DDLUtils.isDatasourceTable(tableDesc) =>
      val tableIdent = state.catalog.resolveTableIdentifier(tableDesc.identifier)
      val provider = tableDesc.provider.get
      val isBuiltin = SnappyContext.isBuiltInProvider(provider) ||
          CatalogObjectType.isGemFireProvider(provider)
      // treat saveAsTable with mode=Append as insertInto for builtin tables and "normal" cases
      // where no explicit bucket/partitioning has been specified
      if (mode == SaveMode.Append && queryOpt.isDefined && (isBuiltin ||
          (tableDesc.bucketSpec.isEmpty && tableDesc.partitionColumnNames.isEmpty)) &&
          state.catalog.tableExists(tableIdent)) {
        internals.newInsertIntoTable(
          table = UnresolvedRelation(tableIdent),
          partition = Map.empty, child = queryOpt.get, overwrite = false, ifNotExists = false)
      } else if (isBuiltin) {
        val tableName = tableIdent.unquotedString
        // dependent tables are stored as comma-separated so don't allow comma in table name
        if (tableName.indexOf(',') != -1) {
          throw new AnalysisException(s"Table '$tableName' cannot contain comma in its name")
        }
        var newOptions = tableDesc.storage.properties +
            (SnappyExternalCatalog.DBTABLE_PROPERTY -> tableName)
        if (CatalogObjectType.isColumnTable(SnappyContext.getProviderType(provider))) {
          // add default batchSize and maxDeltaRows options for column tables
          val parameters = new ExternalStoreUtils.CaseInsensitiveMutableHashMap[String](
            tableDesc.storage.properties)
          if (!parameters.contains(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS)) {
            newOptions += (ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS ->
                ExternalStoreUtils.defaultColumnMaxDeltaRows(state.snappySession).toString)
          }
          if (!parameters.contains(ExternalStoreUtils.COLUMN_BATCH_SIZE)) {
            newOptions += (ExternalStoreUtils.COLUMN_BATCH_SIZE ->
                ExternalStoreUtils.defaultColumnBatchSize(state.snappySession).toString)
          }
        }
        c.copy(tableDesc.copy(storage = tableDesc.storage.copy(properties = newOptions)))
      } else c

    // resolve INSERT INTO/OVERWRITE TABLE(columns) ...
    case i: InsertIntoTable if i.table.isInstanceOf[UnresolvedTableValuedFunction] =>
      val isOverwrite = internals.getOverwriteOption(i)
      val query = i.children.head
      resolveProjection(i.table.asInstanceOf[UnresolvedTableValuedFunction], query,
        s"INSERT ${if (isOverwrite) "OVERWRITE" else "INTO"}") match {
        case (_, d: DMLExternalTable) =>
          // no support for OVERWRITE or PARTITION for this case
          val tableName = d.child match {
            case lr: LogicalRelation if lr.relation.isInstanceOf[JDBCMutableRelation] =>
              " " + lr.relation.asInstanceOf[JDBCMutableRelation].resolvedName
            case _ => ""
          }
          if (isOverwrite) {
            throw new AnalysisException(s"INSERT OVERWRITE not supported with " +
                s"table column specification on row table$tableName")
          }
          if (i.partition.nonEmpty) {
            throw new AnalysisException(s"INSERT with PARTITION not supported with " +
                s"table column specification on row table$tableName")
          }
          d
        case (t, c) =>
          if ((t eq i.table) && (c eq query)) i
          else i.copy(t, i.partition, c, i.overwrite)
      }

    // resolve PUT INTO TABLE(columns) ...
    case p@PutIntoTable(u: UnresolvedTableValuedFunction, child) =>
      resolveProjection(u, child, "PUT INTO") match {
        case (_, d: DMLExternalTable) => d
        case (t, c) => if ((t eq u) && (c eq child)) p else p.copy(table = t, child = c)
      }

    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l: LogicalRelation, _, child, _, _)
      if l.relation.isInstanceOf[SchemaInsertableRelation] && l.resolved && child.resolved =>
      val r = l.relation.asInstanceOf[SchemaInsertableRelation]
      r.insertableRelation(child.output) match {
        case Some(ir) if r eq ir => i
        case Some(ir) =>
          val br = ir.asInstanceOf[BaseRelation]
          val relation = internals.newLogicalRelation(br,
            None, l.catalogTable, isStreaming = false)
          castAndRenameChildOutputForPut(i.copy(table = relation),
            relation.output, br, null, child)
        case None =>
          throw new AnalysisException(s"$l requires that the query in the " +
              "SELECT clause of the INSERT INTO/OVERWRITE statement " +
              "generates the same number of columns as its schema.")
      }

    // Check for PUT
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l: LogicalRelation if l.relation.isInstanceOf[RowInsertableRelation] =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          val expectedOutput = l.output
          if (expectedOutput.size != child.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutputForPut(p, expectedOutput, l.relation, l, child)

        case _ => p
      }

    // Check for DELETE
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case d@DeleteFromTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l: LogicalRelation if l.relation.isInstanceOf[MutableRelation] =>
          val mr = l.relation.asInstanceOf[MutableRelation]
          val keyColumns = mr.getPrimaryKeyColumns(state.snappySession)
          val childOutput = keyColumns.map(col =>
            child.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"$l requires that the query in the " +
                  "WHERE clause of the DELETE FROM statement " +
                  s"must have all the key column(s) ${keyColumns.mkString(",")} but found " +
                  s"${child.output.mkString(",")} instead.")
            })

          val expectedOutput = keyColumns.map(col =>
            l.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"The target table must contain all the" +
                  s" key column(s) ${keyColumns.mkString(",")}. " +
                  s"Actual schema: ${l.output.mkString(",")}")
            })

          castAndRenameChildOutputForPut(d, expectedOutput, l.relation,
            l, Project(childOutput, child))

        case _ => d
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, _, child, _, _)
      if table.resolved && child.resolved => PreprocessTableInsertion(conf).apply(i)
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  def castAndRenameChildOutputForPut[T <: LogicalPlan](
      plan: T,
      expectedOutput: Seq[Attribute],
      relation: BaseRelation,
      newRelation: LogicalRelation,
      child: LogicalPlan): T = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name) {
          actual
        } else {
          // avoid unnecessary copy+cast when inserting DECIMAL types
          // into column table
          actual.dataType match {
            case _: DecimalType
              if expected.dataType.isInstanceOf[DecimalType] &&
                  relation.isInstanceOf[PlanInsertableRelation] => actual
            case _ => Alias(Cast(actual, expected.dataType), expected.name)()
          }
        }
    }

    if (newChildOutput == child.output) {
      plan match {
        case p: PutIntoTable => p.copy(table = newRelation).asInstanceOf[T]
        case d: DeleteFromTable => d.copy(table = newRelation).asInstanceOf[T]
        case _: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case d: DeleteFromTable => d.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case i: InsertIntoTable => internals.withNewChild(i, Project(newChildOutput,
        child)).asInstanceOf[T]
    }
  }
}

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(l: LogicalRelation, query)
        if l.relation.isInstanceOf[RowPutRelation] || l.relation.isInstanceOf[BulkPutRelation] =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case l: LogicalRelation => l.relation
        }
        if (srcRelations.contains(l.relation)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }
      case PutIntoTable(table, _) =>
        throw Utils.analysisException(s"$table does not allow puts.")
      case _ => // OK
    }
  }
}

private[sql] case class ConditionalPreWriteCheck(sparkPreWriteCheck: LogicalPlan => Unit)
    extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan match {
      case PutIntoColumnTable(_, _, _) => // Do nothing
      case _ => sparkPreWriteCheck.apply(plan)
    }
  }
}

/**
 * Unlike Spark's `InsertIntoTable` this plan provides the count of rows
 * inserted as the output.
 *
 * Note that the underlying BaseRelation should always be a [[PlanInsertableRelation]].
 */
case class InsertIntoPlan(logicalRelation: LogicalRelation,
    query: LogicalPlan, overwrite: Boolean) extends LogicalPlan {

  override lazy val output: Seq[Attribute] = AttributeReference("count", LongType)() :: Nil

  override def children: Seq[LogicalPlan] = Nil

  override protected def innerChildren: Seq[QueryPlan[_]] = query :: Nil

  val relation: PlanInsertableRelation =
    logicalRelation.relation.asInstanceOf[PlanInsertableRelation]
}

private[sql] object ResolveInsertIntoPlan extends Rule[LogicalPlan] with SparkSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = internals.logicalPlanResolveDown(plan) {
    case i@InsertIntoTable(l: LogicalRelation, _, query, _, _)
      if l.relation.isInstanceOf[PlanInsertableRelation] && l.resolved && query.resolved =>

      // check that insert with overwrite does not refer to the source table in the query
      val isOverwrite = internals.getOverwriteOption(i)
      if (isOverwrite) {
        query.foreach {
          case lr: LogicalRelation if lr.relation == l.relation =>
            throw new AnalysisException(
              "Cannot insert overwrite into table that is also being read from.")
          case _ =>
        }
      }
      InsertIntoPlan(l, query, isOverwrite)
  }
}

/**
 * Deals with any escape characters in the LIKE pattern in optimization.
 * Does not deal with startsAndEndsWith equivalent of Spark's LikeSimplification
 * so 'a%b' kind of pattern with additional escaped chars will not be optimized.
 */
object LikeEscapeSimplification extends SparkSupport {

  private def addTokenizedLiteral(parser: SnappyParser, s: String): Expression = {
    if (parser ne null) parser.newCachedLiteral(UTF8String.fromString(s), StringType)
    else new TokenLiteral(UTF8String.fromString(s), StringType)
  }

  def simplifyLike(parser: SnappyParser, expr: Expression,
      left: Expression, pattern: String): Expression = {
    val len_1 = pattern.length - 1
    if (len_1 == -1) return EqualTo(left, addTokenizedLiteral(parser, ""))
    val str = new StringBuilder(pattern.length)
    var wildCardStart = false
    var i = 0
    while (i < len_1) {
      pattern.charAt(i) match {
        case '\\' =>
          val c = pattern.charAt(i + 1)
          c match {
            case '_' | '%' | '\\' => // literal char
            case _ => return expr
          }
          str.append(c)
          // if next character is last one then it is literal
          if (i == len_1 - 1) {
            if (wildCardStart) return EndsWith(left, addTokenizedLiteral(parser, str.toString))
            else return EqualTo(left, addTokenizedLiteral(parser, str.toString))
          }
          i += 1
        case '%' if i == 0 => wildCardStart = true
        case '%' | '_' => return expr // wildcards in middle are left as is
        case c => str.append(c)
      }
      i += 1
    }
    pattern.charAt(len_1) match {
      case '%' =>
        if (wildCardStart) Contains(left, addTokenizedLiteral(parser, str.toString))
        else StartsWith(left, addTokenizedLiteral(parser, str.toString))
      case '_' | '\\' => expr
      case c =>
        str.append(c)
        if (wildCardStart) EndsWith(left, addTokenizedLiteral(parser, str.toString))
        else EqualTo(left, addTokenizedLiteral(parser, str.toString))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = internals.logicalPlanResolveExpressions(plan) {
    case l@Like(left, Literal(pattern, StringType)) =>
      simplifyLike(null, l, left, pattern.toString)
  }
}

case class MarkerForCreateTableAsSelect(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class BypassRowLevelSecurity(child: LogicalFilter) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
