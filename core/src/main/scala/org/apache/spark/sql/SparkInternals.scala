/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.lang.reflect.Method

import com.google.common.cache.CacheBuilder
import io.snappydata.Property
import io.snappydata.sql.catalog.SnappyExternalCatalog
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodegenContext, ExprCode, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, ExpressionInfo, FrameType, Generator, NamedExpression, NullOrdering, SortDirection, SortOrder, SpecifiedWindowFrame}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.execution.ui.SQLTab
import org.apache.spark.sql.execution.{CacheManager, CodegenSparkFallback, DataSourceScanExec, PartitionedDataSourceScan, RowDataSourceScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.hive.{SnappyAnalyzer, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.internal.{SQLConf, SnappySharedState}
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.streaming.LogicalDStreamPlan
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.apache.spark.status.api.v1.RDDStorageInfo
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.ui.{SparkUI, WebUITab}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}

/**
 * Common interface for Spark internal API used by the core module.
 *
 * Note that this interface only intends to achieve source-level
 * compatibility meaning that entire core module with the specific
 * implementation of this interface has to be re-compiled in entirety
 * for separate Spark versions and one cannot just combine core module
 * compiled for a Spark version with an implementation of this
 * interface for another Spark version.
 */
trait SparkInternals extends Logging {

  final val emptyFunc: String => String = _ => ""

  /**
   * Global instance of EmptyRDD used in canonicalized versions of plans.
   */
  lazy val EMPTY_RDD = new EmptyRDD[Any](SparkContext.getActive.get)

  protected final lazy val dataSourceCache =
    CacheBuilder.newBuilder().maximumSize(dsCacheSize).build[(String, ClassLoader), Class[_]]()

  if (version != SparkSupport.DEFAULT_VERSION) {
    logInfo(s"SnappyData: loading support for Spark $version")
  }

  protected final def dsCacheSize: Int = {
    SparkEnv.get match {
      case null => Property.DataSourceCacheSize.defaultValue.get
      case env => Property.DataSourceCacheSize.get(env.conf)
    }
  }

  /**
   * Version of this implementation. This should always match
   * the result of SparkContext.version for current SparkContext.
   */
  def version: String

  /**
   * Remove any cached data of Dataset.persist for given plan.
   */
  def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit

  /**
   * Register an inbuilt function in the session function registry.
   */
  def registerFunction(sessionState: SnappySessionState, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit

  /**
   * Add a mutable state variable to given [[CodegenContext]] and return the variable name.
   */
  def addClassField(ctx: CodegenContext, javaType: String,
      varPrefix: String, initFunc: String => String = emptyFunc,
      forceInline: Boolean = false, useFreshName: Boolean = true): String

  /**
   * Get all the inline class fields in the given CodegenContext.
   */
  def getInlinedClassFields(ctx: CodegenContext): (Seq[(String, String)], Seq[String])

  /**
   * Adds a function to the generated class. In newer Spark versions, if the code for outer class
   * grows too large, the function will be inlined into a new private, inner class,
   * and a class-qualified name for the function will be returned.
   */
  def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean = false): String

  /**
   * Returns true if a given function has already been added to the outer class.
   */
  def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean

  /**
   * Split the generated code for given expressions into multiple methods assuming
   * [[CodegenContext.INPUT_ROW]] has been used (else return inline expression code).
   */
  def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String

  /**
   * Reset CodegenContext's copyResult to false if required (skipped in newer Spark versions).
   */
  def resetCopyResult(ctx: CodegenContext): Unit

  /**
   * Check if the current expression is a predicate sub-query.
   */
  def isPredicateSubquery(expr: Expression): Boolean

  /**
   * Create a new IN expression for a subquery. Older Spark versions handle
   * it as a regular IN expression while newer ones use a separate InSubquery.
   */
  def newInSubquery(expr: Expression, query: LogicalPlan): Expression

  /**
   * Make a copy of given predicate sub-query with new plan and [[ExprId]].
   */
  def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan, newExprId: ExprId): Expression

  /**
   * Create a new unresolved regular expression.
   */
  def newUnresolvedRegex(regex: String, table: Option[String], caseSensitive: Boolean): Expression

  /**
   * Create a new lambda function with given arguments.
   */
  def newLambdaFunction(expression: Expression, args: Seq[Expression]): Expression

  // scalastyle:off

  /**
   * Create an instance of [[ColumnTableScan]] for the current Spark version.
   *
   * The primary reason is the difference between "sameResult" implementation which is
   * final in newer Spark versions and needs to override doCanonicalize instead.
   */
  def columnTableScan(output: Seq[Attribute], dataRDD: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]], numBuckets: Int, partitionColumns: Seq[Expression],
      partitionColumnAliases: Seq[Seq[Attribute]], baseRelation: PartitionedDataSourceScan,
      relationSchema: StructType, allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference], caseSensitive: Boolean,
      isSampleReservoirAsRegion: Boolean = false): ColumnTableScan

  // scalastyle:on

  /**
   * Create an instance of [[RowTableScan]] for the current Spark version.
   *
   * The primary reason is the difference between "sameResult" implementation which is
   * final in newer Spark versions and needs to override doCanonicalize instead.
   */
  def rowTableScan(output: Seq[Attribute], schema: StructType, dataRDD: RDD[Any], numBuckets: Int,
      partitionColumns: Seq[Expression], partitionColumnAliases: Seq[Seq[Attribute]],
      table: String, baseRelation: PartitionedDataSourceScan, caseSensitive: Boolean): RowTableScan

  /**
   * Compile the given [[SparkPlan]] using whole-stage code generation and return
   * the generated code along with the [[CodegenContext]] use for code generation.
   */
  def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec

  /**
   * Create a new immutable map whose keys are case-insensitive from a given map.
   */
  def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String]

  /**
   * Remove static handler having given path from [[SparkUI]].
   */
  def detachHandler(ui: SparkUI, path: String): Unit

  /**
   * Remove all SQLTabs except the one passed (which can be null).
   */
  def removeSQLTabs(sparkContext: SparkContext, except: Option[WebUITab]): Unit = {
    sparkContext.ui match {
      case Some(ui) =>
        val skipTab = if (except.isEmpty) null else except.get
        ui.getTabs.foreach {
          case tab: SQLTab if tab ne skipTab =>
            ui.detachTab(tab)
            detachHandler(ui, "/static/sql")
          case _ =>
        }
      case _ =>
    }
  }

  /**
   * Create a new SQL listener with SnappyData extensions and attach to the SparkUI.
   * The extension provides handling of:
   * <p>
   * a) combining the two part execution with CachedDataFrame where first execution
   * does the caching ("prepare" phase) along with the actual execution while subsequent
   * executions only do the latter
   * <p>
   * b) shortens the SQL string to display properly in the UI (CachedDataFrame already
   * takes care of posting the SQL string rather than method name unlike Spark).
   * <p>
   * This is invoked before initialization of SharedState for Spark releases
   * where listener is attached independently of SharedState before latter is created
   * while it is invoked after initialization of SharedState for newer Spark versions.
   */
  def createAndAttachSQLListener(sparkContext: SparkContext): Unit

  /**
   * Create a new global instance of [[SnappySharedState]].
   */
  def newSharedState(sparkContext: SparkContext): SnappySharedState

  /**
   * Clear any global SQL listener.
   */
  def clearSQLListener(): Unit

  /**
   * Create a SQL string appropriate for a persisted VIEW plan and storage in catalog
   * from a given [[LogicalPlan]] for the VIEW.
   */
  def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String

  /**
   * Create a [[LogicalPlan]] for CREATE VIEW.
   */
  def createView(desc: CatalogTable, output: Seq[Attribute], child: LogicalPlan): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for CREATE FUNCTION.
   */
  def newCreateFunctionCommand(schemaName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for DESCRIBE TABLE.
   */
  def newDescribeTableCommand(table: TableIdentifier, partitionSpec: Map[String, String],
      isExtended: Boolean, isFormatted: Boolean): RunnableCommand

  /**
   * Create a [[LogicalPlan]] for CLEAR CACHE.
   */
  def newClearCacheCommand(): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for CREATE TABLE ... LIKE
   */
  def newCreateTableLikeCommand(targetIdent: TableIdentifier, sourceIdent: TableIdentifier,
      location: Option[String], allowExisting: Boolean): RunnableCommand

  /**
   * Lookup a relation in catalog.
   */
  def lookupRelation(catalog: SessionCatalog, name: TableIdentifier,
      alias: Option[String]): LogicalPlan

  /**
   * Resolve Maven coordinates for a package, cache the jars and return the required CLASSPATH.
   */
  def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String

  /**
   * Create a copy of [[Attribute]] as [[AttributeReference]] with given arguments.
   */
  def toAttributeReference(attr: Attribute)(name: String = attr.name,
      dataType: DataType = attr.dataType, nullable: Boolean = attr.nullable,
      metadata: Metadata = attr.metadata, exprId: ExprId = attr.exprId): AttributeReference

  /**
   * Create a new instance of [[AttributeReference]]
   */
  def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String],
      isGenerated: Boolean = false): AttributeReference

  /**
   * Create a new concrete instance of [[ErrorEstimateAttribute]].
   */
  def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, realExprId: ExprId,
      exprId: ExprId = NamedExpression.newExprId,
      qualifier: Seq[String] = Nil): ErrorEstimateAttribute

  /**
   * Create a new concrete instance of [[ApproxColumnExtractor]].
   */
  def newApproxColumnExtractor(child: Expression, name: String, ordinal: Int,
      dataType: DataType, nullable: Boolean, exprId: ExprId = NamedExpression.newExprId,
      qualifier: Seq[String] = Nil): ApproxColumnExtractor

  /**
   * Create a new concrete instance of [[TaggedAttribute]].
   */
  def newTaggedAttribute(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId = NamedExpression.newExprId,
      qualifier: Seq[String] = Nil): TaggedAttribute

  /**
   * Create a new concrete instance of [[TaggedAlias]].
   */
  def newTaggedAlias(tag: TransformableTag, child: Expression, name: String,
      exprId: ExprId = NamedExpression.newExprId, qualifier: Seq[String] = Nil): TaggedAlias

  // scalastyle:off

  /**
   * Create a new concrete instance of [[ClosedFormColumnExtractor]].
   */
  def newClosedFormColumnExtractor(child: Expression, name: String, confidence: Double,
      confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
      behavior: HAC.Type, nullable: Boolean, exprId: ExprId = NamedExpression.newExprId,
      qualifier: Seq[String] = Nil): ClosedFormColumnExtractor

  // scalastyle:on

  /**
   * Create a copy of [[InsertIntoTable]] plan with a new child.
   */
  def withNewChild(insert: InsertIntoTable, newChild: LogicalPlan): InsertIntoTable

  /**
   * Create a new [[InsertIntoTable]] plan.
   */
  def newInsertIntoTable(table: LogicalPlan, partition: Map[String, Option[String]],
      child: LogicalPlan, overwrite: Boolean, ifNotExists: Boolean): InsertIntoTable

  /**
   * Return true if overwrite is enabled in the insert plan else false.
   */
  def getOverwriteOption(insert: InsertIntoTable): Boolean

  /**
   * Create an expression for GROUPING SETS.
   */
  def newGroupingSet(groupingSets: Seq[Seq[Expression]], groupByExprs: Seq[Expression],
      child: LogicalPlan, aggregations: Seq[NamedExpression]): LogicalPlan

  /**
   * Create an alias for a sub-query.
   */
  def newSubqueryAlias(alias: String, child: LogicalPlan,
      view: Option[TableIdentifier] = None): LogicalPlan

  /**
   * Get view, if defined, or else alias name of a SubqueryAlias.
   */
  def getViewFromAlias(q: SubqueryAlias): Option[TableIdentifier]

  /**
   * Create an alias with given parameters and optionally copying other fields from existing Alias.
   */
  def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId = NamedExpression.newExprId, qualifier: Seq[String] = Nil): Alias

  /**
   * Create a plan for column aliases in a table/sub-query/...
   * Not supported by older Spark versions.
   */
  def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan

  /**
   * Create a [[SortOrder]].
   */
  def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder

  /**
   * Create a new [[LogicalPlan]] for REPARTITION.
   */
  def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression

  /**
   * Create a new unresolved table value function.
   */
  def newUnresolvedTableValuedFunction(functionName: String, functionArgs: Seq[Expression],
      outputNames: Seq[String]): UnresolvedTableValuedFunction

  /**
   * Create a new unresolved HAVING plan (is a simple Filter in Spark < 2.4).
   */
  def newUnresolvedHaving(predicate: Expression, child: LogicalPlan): LogicalPlan

  /**
   * Create a new frame boundary. This is a FrameBoundary is older Spark versions
   * while newer ones use an Expression instead.
   */
  def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression] = None): Any

  /**
   * Create a new [[SpecifiedWindowFrame]] given the [[FrameType]] and start/end frame
   * boundaries as returned by [[newFrameBoundary]].
   */
  def newSpecifiedWindowFrame(frameType: FrameType,
      frameStart: Any, frameEnd: Any): SpecifiedWindowFrame

  /**
   * Create a new wrapper [[LogicalPlan]] that encapsulates an arbitrary set of hints.
   */
  def newLogicalPlanWithHints(child: LogicalPlan, hints: Map[String, String]): LogicalPlan

  /**
   * Create a new `UnresolvedHint` plan if available in this version of Spark else return `child`.
   */
  def newUnresolvedHint(name: String, parameters: Seq[Any], child: LogicalPlan): LogicalPlan

  /**
   * Create a new TABLESAMPLE operator.
   */
  def newTableSample(lowerBound: Double, upperBound: Double, withReplacement: Boolean,
      seed: Long, child: LogicalPlan): Sample

  /**
   * Return true if the given LogicalPlan encapsulates a child plan with query hint(s).
   */
  def isHintPlan(plan: LogicalPlan): Boolean

  /**
   * If the given plan encapsulates query hints, then return the hint type and name pairs.
   */
  def getHints(plan: LogicalPlan): Map[String, String]

  /**
   * Return true if current plan has been explicitly marked for broadcast and false otherwise.
   */
  def isBroadcastable(plan: LogicalPlan): Boolean

  /**
   * Create a new OneRowRelation.
   */
  def newOneRowRelation(): LogicalPlan

  /**
   * Create a new [[LogicalPlan]] for GENERATE.
   */
  def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan

  /**
   * Write a DataFrame to a DataSource.
   */
  def writeToDataSource(ds: DataSource, mode: SaveMode, data: Dataset[Row]): BaseRelation

  /**
   * Create a new [[LogicalRelation]].
   */
  def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation

  /**
   * Create a DataFrame out of an RDD of InternalRows.
   */
  def internalCreateDataFrame(session: SparkSession, catalystRows: RDD[InternalRow],
      schema: StructType, isStreaming: Boolean = false): Dataset[Row]

  /**
   * Create a new [[RowDataSourceScanExec]] with the given parameters.
   */
  def newRowDataSourceScanExec(fullOutput: Seq[Attribute], requiredColumnsIndex: Seq[Int],
      filters: Seq[Filter], handledFilters: Seq[Filter], rdd: RDD[InternalRow],
      metadata: Map[String, String], relation: BaseRelation,
      tableIdentifier: Option[TableIdentifier]): RowDataSourceScanExec

  /**
   * Get the table identifier for given DataSourceScanExec.
   */
  def tableIdentifier(scan: DataSourceScanExec): Option[TableIdentifier]

  /**
   * Create a new [[CodegenSparkFallback]] with the given child.
   */
  def newCodegenSparkFallback(child: SparkPlan, session: SnappySession): CodegenSparkFallback

  /**
   * Create a new [[LogicalDStreamPlan]] with the given parameters.
   */
  def newLogicalDStreamPlan(output: Seq[Attribute], stream: DStream[InternalRow],
      streamingSnappy: SnappyStreamingContext): LogicalDStreamPlan

  /**
   * Create a new CatalogDatabase given the parameters. Newer Spark releases require a URI
   * for locationUri so the given string will be converted to URI for those Spark versions.
   */
  def newCatalogDatabase(name: String, description: String,
      locationUri: String, properties: Map[String, String]): CatalogDatabase

  /** Get the locationURI for CatalogDatabase in String format. */
  def catalogDatabaseLocationURI(database: CatalogDatabase): String

  // scalastyle:off

  /**
   * Create a new CatalogTable given the parameters. The primary constructor
   * of the class has seen major changes across Spark versions.
   */
  def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[AnyRef], viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable

  // scalastyle:on

  /** Get the viewOriginalText of CataLogTable or None if not present. */
  def catalogTableViewOriginalText(catalogTable: CatalogTable): Option[String]

  /** Get the ignoredProperties map of CataLogTable or empty map if not present. */
  def catalogTableIgnoredProperties(catalogTable: CatalogTable): Map[String, String]

  /** Return a new CatalogTable with updated viewOriginalText if possible. */
  def newCatalogTableWithViewOriginalText(catalogTable: CatalogTable,
      viewOriginalText: Option[String]): CatalogTable

  /**
   * Create a new CatalogStorageFormat given the parameters.
   */
  def newCatalogStorageFormat(locationUri: Option[String], inputFormat: Option[String],
      outputFormat: Option[String], serde: Option[String], compressed: Boolean,
      properties: Map[String, String]): CatalogStorageFormat

  /** Get the string representation of locationUri field of CatalogStorageFormat. */
  def catalogStorageFormatLocationUri(storageFormat: CatalogStorageFormat): Option[String]

  /** Serialize a CatalogTablePartition to InternalRow */
  def catalogTablePartitionToRow(partition: CatalogTablePartition,
      partitionSchema: StructType, defaultTimeZoneId: String): InternalRow

  /** Query catalog to load dynamic partitions defined in given Spark table. */
  def loadDynamicPartitions(externalCatalog: ExternalCatalog, schema: String,
      table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean,
      numDP: Int, holdDDLTime: Boolean): Unit

  /** Alter table schema in the ExternalCatalog if possible else throw an exception */
  def alterTableSchema(externalCatalog: ExternalCatalog, schemaName: String,
      table: String, newSchema: StructType): Unit

  /**
   * Alter table statistics in the ExternalCatalog if possible else throw an exception.
   * The `stats` argument is an optional Statistics (for Spark < 2.2) or CatalogStatistics object.
   */
  def alterTableStats(externalCatalog: ExternalCatalog, schema: String, table: String,
      stats: Option[AnyRef]): Unit

  /** Alter function definition in the ExternalCatalog if possible else throw an exception */
  def alterFunction(externalCatalog: ExternalCatalog, schema: String,
      function: CatalogFunction): Unit

  /** Convert a ColumnStat (or CatalogColumnStat for Spark >= 2.4) to a map. */
  def columnStatToMap(stat: Any, colName: String, dataType: DataType): Map[String, String]

  /** Convert a map created by [[columnStatToMap]] to ColumnStat or CatalogColumnStat. */
  def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[AnyRef]

  /**
   * Create a Statistics/CatalogStatistics object from given arguments. The `colStats` argument
   * is a map of string to ColumnStat(Spark < 2.4)/CatalogColumnStat
   */
  def toCatalogStatistics(sizeInBytes: BigInt, rowCount: Option[BigInt],
      colStats: Map[String, AnyRef]): AnyRef

  /**
   * Create a new instance of SnappyHiveExternalCatalog. The method overrides in
   * ExternalCatalog have changed across Spark versions.
   */
  def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog

  /**
   * Create a new instance of SmartConnectorExternalCatalog. The method overrides in
   * ExternalCatalog have changed across Spark versions.
   */
  def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog

  /** Lookup the data source for a given provider (with caching). */
  final def lookupDataSource(provider: String, conf: => SQLConf): Class[_] = {
    // account for SQLConf overrides that may differ across sessions
    val providerKey = provider.toLowerCase match {
      case "orc" => "orc:" + conf.getConfString("spark.sql.orc.impl", "native")
      case "com.databricks.spark.avro" => "com.databricks.spark.avro:" +
          conf.getConfString("spark.sql.legacy.replaceDatabricksSparkAvro.enabled", "true")
      case p => p
    }
    val key = providerKey -> Utils.getContextOrSparkClassLoader
    // avoid callable object creation if possible
    dataSourceCache.getIfPresent(key) match {
      case null => dataSourceCache.get(key, new java.util.concurrent.Callable[Class[_]] {
        override def call(): Class[_] = basicLookupDataSource(provider, conf)
      })
      case c => c
    }
  }

  /** Invoke Spark's `DataSource.lookupDataSource` method. */
  protected def basicLookupDataSource(provider: String, conf: => SQLConf): Class[_]

  /**
   * Create a new shuffle exchange plan.
   */
  def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange

  /**
   * Return true if the given plan is a ShuffleExchange.
   */
  def isShuffleExchange(plan: SparkPlan): Boolean

  /**
   * Get the classOf ShuffleExchange operator.
   */
  def classOfShuffleExchange(): Class[_]

  /**
   * Get the [[Statistics]] for a given [[LogicalPlan]].
   */
  def getStatistics(plan: LogicalPlan): Statistics

  /**
   * Return true if the given [[AggregateFunction]] support partial result aggregation.
   */
  def supportsPartial(aggregate: AggregateFunction): Boolean

  /**
   * Create a physical [[SparkPlan]] for an [[AggregateFunction]] that does not support
   * partial result aggregation ([[supportsPartial]] is false).
   */
  def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression], planChild: () => SparkPlan): Seq[SparkPlan]

  /**
   * Compile given generated code assuming it results in an implemenation of [[GeneratedClass]].
   */
  def compile(code: CodeAndComment): GeneratedClass

  /**
   * Create a new [[JSONOptions]] object given the parameters.
   */
  def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions

  /**
   * Create a new instance of [[SnappySessionState]] appropriate for the current Spark version.
   */
  def newSnappySessionState(snappySession: SnappySession): SnappySessionState

  /**
   * Return the Spark plan for check pre-conditions before a write operation.
   */
  def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit

  /**
   * Return list of HiveConditionalStrategies to be applied when hive external catalog is enabled.
   */
  def hiveConditionalStrategies(sessionState: SnappySessionState): Seq[Strategy]

  /**
   * Create a new SnappyData extended CacheManager to clear cached plans on cached data changes.
   */
  def newCacheManager(): CacheManager

  /**
   * Create a new SQLConf entry with registration actions for the given key.
   */
  def buildConf(key: String): ConfigBuilder

  /**
   * Get the global list of cached RDDs (as list of [[RDDStorageInfo]]).
   */
  def getCachedRDDInfos(context: SparkContext): Seq[RDDStorageInfo]

  /**
   * Get the return data type of given java method.
   * A result of NullType indicates a possible StructType, so caller should check for the same.
   */
  def getReturnDataType(method: Method): DataType

  /**
   * Create a new ExprCode with given arguments.
   */
  def newExprCode(code: String, isNull: String, value: String, dt: DataType): ExprCode

  /**
   * Make a copy of ExprCode with given new arguments.
   */
  def copyExprCode(ev: ExprCode, code: String = null, isNull: String = null,
      value: String = null, dt: DataType = null): ExprCode

  /**
   * Reset the code field of [[ExprCode]] to empty code block.
   */
  def resetCode(ev: ExprCode): Unit

  /**
   * Get the string for isNull field of [[ExprCode]].
   */
  def exprCodeIsNull(ev: ExprCode): String

  /**
   * Set the isNull field of [[ExprCode]].
   */
  def setExprCodeIsNull(ev: ExprCode, isNull: String): Unit

  /**
   * Get the string for value field of [[ExprCode]].
   */
  def exprCodeValue(ev: ExprCode): String

  /**
   * Get the string for java type for given [[DataType]].
   */
  def javaType(dt: DataType, ctx: CodegenContext): String

  /**
   * Get the java type of boxed type for given type.
   */
  def boxedType(javaType: String, ctx: CodegenContext): String

  /**
   * Get the string form of default value for given [[DataType]].
   */
  def defaultValue(dt: DataType, ctx: CodegenContext): String

  /**
   * Returns true if the Java type has a special accessor and setter in [[InternalRow]].
   */
  def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean

  /**
   * Returns the name used in accessor and setter for a Java primitive type.
   */
  def primitiveTypeName(javaType: String, ctx: CodegenContext): String

  /**
   * Returns the specialized code to access a value from `inputRow` at `ordinal`.
   */
  def getValue(input: String, dataType: DataType, ordinal: String, ctx: CodegenContext): String

  /**
   * List of any optional plans to be executed in the QueryExecution.preparations phase.
   */
  def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]]

  /**
   * Create a new instance of [[Pivot]] plan.
   */
  def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot

  /**
   * Create a copy of [[Pivot]] plan with a new set of groupBy expressions.
   */
  def copyPivot(pivot: Pivot, groupByExprs: Seq[NamedExpression]): Pivot

  /**
   * Create a new instance of [[Intersect]] plan.
   */
  def newIntersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Intersect

  /**
   * Create a new instance of [[Except]] plan.
   */
  def newExcept(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Except

  /**
   * Create a plan for explain command.
   */
  def newExplainCommand(logicalPlan: LogicalPlan, extended: Boolean,
      codegen: Boolean, cost: Boolean): LogicalPlan

  /**
   * Create a plan for TRUNCATE TABLE for non-snappy tables.
   * Returns `None` if truncate is not supported in this version of Spark.
   */
  def newTruncateTableCommand(tableName: TableIdentifier): Option[RunnableCommand]

  /**
   * Get the internal cached RDD for an in-memory relation.
   */
  def cachedColumnBuffers(relation: InMemoryRelation): RDD[_]

  /**
   * Add SnappyData custom string promotion rules to deal with ParamLiterals.
   */
  def addStringPromotionRules(rules: Seq[Rule[LogicalPlan]],
      analyzer: SnappyAnalyzer, conf: SQLConf): Seq[Rule[LogicalPlan]]

  /**
   * SPARK-21865 changed the way distribution/partitioning semantics works requiring
   * join and similar plans to use HashClusteredDistribution instead of ClusteredDistribution
   * else there might be a mismatch between required child distribution and partitioning
   * since latter no longer checks for `compatibleWith` with former.
   */
  def newHashClusteredDistribution(expressions: Seq[Expression],
      requiredNumPartitions: Option[Int] = None): Distribution

  /**
   * Create a new HashPartitioning given a Clustered/HashClusteredDistribution.
   */
  def newClusteredPartitioning(distribution: Distribution, numPartitions: Int): Partitioning

  /**
   * Create table definition in the catalog.
   */
  def createTable(catalog: SessionCatalog, tableDefinition: CatalogTable,
      ignoreIfExists: Boolean, validateLocation: Boolean): Unit = {
    catalog.createTable(tableDefinition, ignoreIfExists)
  }

  /**
   * Transform down a [[LogicalPlan]] during analysis phase.
   * This translates to resolveOperatorsDown in Spark 2.4.x
   * while it uses transformDown in earlier versions.
   */
  def logicalPlanResolveDown(plan: LogicalPlan)(
      rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = plan.transformDown(rule)

  /**
   * Transform up a [[LogicalPlan]] during analysis phase.
   * This translates to resolveOperatorsUp in Spark 2.4.x
   * while it uses transformUp in earlier versions.
   */
  def logicalPlanResolveUp(plan: LogicalPlan)(
      rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = plan.transformUp(rule)

  /**
   * Transform all expressions in a [[LogicalPlan]] during analysis phase.
   * This translates to resolveExpressions in Spark 2.4.x
   * while it uses transformAllExpressions in earlier versions.
   */
  def logicalPlanResolveExpressions(plan: LogicalPlan)(
      rule: PartialFunction[Expression, Expression]): LogicalPlan = {
    plan.transformAllExpressions(rule)
  }
}

/**
 * Enumeration for frame boundary type to provie a common way of expressing it due to
 * major change in frame boundary handling across Spark versions.
 */
object FrameBoundaryType extends Enumeration {
  type Type = Value

  val CurrentRow, UnboundedPreceding, UnboundedFollowing, ValuePreceding, ValueFollowing = Value
}
