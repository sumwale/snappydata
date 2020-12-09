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

package org.apache.spark.sql.internal

import java.lang.reflect.Method

import scala.util.control.NonFatal

import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import io.snappydata.{HintName, Property, QueryHint}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, ExprCode, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CurrentRow, ExprId, Expression, ExpressionInfo, FrameBoundary, FrameType, Generator, In, ListQuery, Literal, NamedExpression, NullOrdering, PredicateSubquery, SortDirection, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, ValueFollowing, ValuePreceding}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, SQLBuilder, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.command.{ClearCacheCommand, CreateFunctionCommand, CreateTableLikeCommand, DescribeTableCommand, ExplainCommand, RunnableCommand}
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.ui.{SQLTab, SnappySQLListener}
import org.apache.spark.sql.hive.{HiveAccessUtil, HiveConditionalRule, HiveConditionalStrategy, HiveSessionCatalog, SnappyAnalyzer, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.sources.{BaseRelation, Filter, JdbcExtendedUtils, ResolveQueryHints}
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, OutputMode, ProcessingTime, SnappyStreamingQueryManager, StreamingQuery, StreamingQueryManager, Trigger}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.apache.spark.status.api.v1.RDDStorageInfo
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark.{SparkConf, SparkContext, SparkException}

/**
 * Base implementation of [[SparkInternals]] for Spark 2.1.x releases.
 */
class Spark21Internals(override val version: String) extends SparkInternals {

  private[this] lazy val caseInsensitiveMapCons = {
    val cons = Utils.classForName("org.apache.spark.sql.catalyst.util.CaseInsensitiveMap")
        .getDeclaredConstructor(classOf[Map[_, _]])
    cons.setAccessible(true)
    cons
  }

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, blocking)
  }

  override def registerFunction(sessionState: SnappySessionState, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit = {
    sessionState.functionRegistry.registerFunction(name.unquotedString, info, function)
  }

  override def addClassField(ctx: CodegenContext, javaType: String,
      varPrefix: String, initFunc: String => String,
      forceInline: Boolean, useFreshName: Boolean): String = {
    val variableName = if (useFreshName) ctx.freshName(varPrefix) else varPrefix
    ctx.addMutableState(javaType, variableName, initFunc(variableName))
    variableName
  }

  override def getInlinedClassFields(ctx: CodegenContext): (Seq[(String, String)], Seq[String]) = {
    ctx.mutableStates.map(t => t._1 -> t._2) -> ctx.mutableStates.map(_._3)
  }

  override def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean): String = {
    ctx.addNewFunction(funcName, funcCode)
    funcName
  }

  override def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean = {
    ctx.addedFunctions.contains(funcName)
  }

  override def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String = {
    ctx.splitExpressions(ctx.INPUT_ROW, expressions)
  }

  override def resetCopyResult(ctx: CodegenContext): Unit = ctx.copyResult = false

  override def isPredicateSubquery(expr: Expression): Boolean =
    expr.isInstanceOf[PredicateSubquery]

  override def newInSubquery(expr: Expression, query: LogicalPlan): Expression = {
    In(expr, ListQuery(query) :: Nil)
  }

  override def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan,
      newExprId: ExprId): Expression = {
    expr.asInstanceOf[PredicateSubquery].copy(plan = newPlan, exprId = newExprId)
  }

  override def newUnresolvedRegex(regexPattern: String, table: Option[String],
      caseSensitive: Boolean): Expression = {
    throw new ParseException(s"UnresolvedRegex not supported in Spark $version")
  }

  override def newLambdaFunction(expression: Expression, args: Seq[Expression]): Expression = {
    throw new ParseException(s"Lambda functions not supported in Spark $version")
  }

  // scalastyle:off

  override def columnTableScan(output: Seq[Attribute], dataRDD: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]], numBuckets: Int, partitionColumns: Seq[Expression],
      partitionColumnAliases: Seq[Seq[Attribute]], baseRelation: PartitionedDataSourceScan,
      relationSchema: StructType, allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference], caseSensitive: Boolean,
      isSampleReservoirAsRegion: Boolean): ColumnTableScan = {
    new ColumnTableScan21(output, dataRDD, otherRDDs, numBuckets, partitionColumns,
      partitionColumnAliases, baseRelation, relationSchema, allFilters, schemaAttributes,
      caseSensitive, isSampleReservoirAsRegion)
  }

  // scalastyle:on

  override def rowTableScan(output: Seq[Attribute], schema: StructType, dataRDD: RDD[Any],
      numBuckets: Int, partitionColumns: Seq[Expression],
      partitionColumnAliases: Seq[Seq[Attribute]], table: String,
      baseRelation: PartitionedDataSourceScan, caseSensitive: Boolean): RowTableScan = {
    new RowTableScan21(output, schema, dataRDD, numBuckets, partitionColumns,
      partitionColumnAliases, JdbcExtendedUtils.toLowerCase(table), baseRelation, caseSensitive)
  }

  override def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec = {
    WholeStageCodegenExec(plan)
  }

  override def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String] = {
    // versions >= 2.1.2 use CaseInsensitiveMap.apply() so use reflection here
    if (map.isEmpty || map.getClass.getName.contains("CaseInsensitiveMap")) map
    else caseInsensitiveMapCons.newInstance(map).asInstanceOf[Map[String, String]]
  }

  override def detachHandler(ui: SparkUI, path: String): Unit = {
    ui.removeStaticHandler(path)
  }

  def createAndAttachSQLListener(sparkContext: SparkContext): Unit = {
    // if the call is done the second time, then attach in embedded mode
    // too since this is coming from ToolsCallbackImpl
    val (forceAttachUI, listener, old) = SparkSession.sqlListener.get() match {
      case l: SnappySQLListener => (true, l, null) // already set
      case l =>
        val listener = new SnappySQLListener(sparkContext.conf, l)
        if (SparkSession.sqlListener.compareAndSet(l, listener)) {
          sparkContext.listenerBus.addListener(listener)
          if (l ne null) sparkContext.listenerBus.removeListener(l)
        }
        (false, listener, l)
    }
    // embedded mode attaches SQLTab later via ToolsCallbackImpl that also
    // takes care of injecting any authentication module if configured
    sparkContext.ui match {
      case Some(ui) if forceAttachUI || !SnappyContext.getClusterMode(sparkContext)
          .isInstanceOf[SnappyEmbeddedMode] =>
        // clear the previous SQLTab, if any
        if (old ne null) {
          removeSQLTabs(sparkContext, except = None)
        }
        new SQLTab(listener, ui)
      case _ =>
    }
  }

  override def newSharedState(sparkContext: SparkContext): SnappySharedState = {
    new SnappySharedState21(sparkContext)
  }

  def clearSQLListener(): Unit = {
    SparkSession.sqlListener.set(null)
  }

  override def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String = {
    val viewSQL = new SQLBuilder(plan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }
    viewSQL
  }

  override def createView(desc: CatalogTable, output: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = child

  override def newCreateFunctionCommand(dbName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan = {
    if (ignoreIfExists) {
      throw new ParseException(s"CREATE FUNCTION does not support IF NOT EXISTS in Spark $version")
    }
    if (replace) {
      throw new ParseException(s"CREATE FUNCTION does not support REPLACE in Spark $version")
    }
    CreateFunctionCommand(dbName, functionName, className, resources, isTemp)
  }

  override def newDescribeTableCommand(table: TableIdentifier,
      partitionSpec: Map[String, String], isExtended: Boolean,
      isFormatted: Boolean): RunnableCommand = {
    DescribeTableCommand(table, partitionSpec, isExtended, isFormatted)
  }

  override def newCreateTableLikeCommand(targetIdent: TableIdentifier,
      sourceIdent: TableIdentifier, location: Option[String],
      allowExisting: Boolean): RunnableCommand = {
    if (location.isDefined) {
      throw new ParseException(s"CREATE TABLE LIKE does not support LOCATION in Spark $version")
    }
    CreateTableLikeCommand(targetIdent, sourceIdent, allowExisting)
  }

  override def lookupRelation(catalog: SessionCatalog, name: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    catalog.lookupRelation(name, alias)
  }

  override def newClearCacheCommand(): LogicalPlan = ClearCacheCommand

  override def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String = {
    SparkSubmitUtils.resolveMavenCoordinates(coordinates, remoteRepos, ivyPath, exclusions)
  }

  override def toAttributeReference(attr: Attribute)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata,
      exprId: ExprId): AttributeReference = {
    AttributeReference(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId, qualifier = attr.qualifier, isGenerated = attr.isGenerated)
  }

  override def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String],
      isGenerated: Boolean): AttributeReference = {
    AttributeReference(name, dataType, nullable, metadata)(exprId,
      qualifier.headOption, isGenerated)
  }

  override def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, realExprId: ExprId, exprId: ExprId,
      qualifier: Seq[String]): ErrorEstimateAttribute = {
    ErrorEstimateAttribute21(name, dataType, nullable, metadata, realExprId)(
      exprId, qualifier.headOption)
  }

  override def newApproxColumnExtractor(child: Expression, name: String, ordinal: Int,
      dataType: DataType, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ApproxColumnExtractor = {
    ApproxColumnExtractor21(child, name, ordinal, dataType, nullable)(exprId, qualifier.headOption)
  }

  override def newTaggedAttribute(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String]): TaggedAttribute = {
    TaggedAttribute21(tag, name, dataType, nullable, metadata)(exprId, qualifier.headOption)
  }

  override def newTaggedAlias(tag: TransformableTag, child: Expression, name: String,
      exprId: ExprId, qualifier: Seq[String]): TaggedAlias = {
    TaggedAlias21(tag, child, name)(exprId, qualifier.headOption)
  }

  // scalastyle:off

  override def newClosedFormColumnExtractor(child: Expression, name: String, confidence: Double,
      confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
      behavior: HAC.Type, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ClosedFormColumnExtractor = {
    ClosedFormColumnExtractor21(child, name, confidence, confFactor, aggType, error,
      dataType, behavior, nullable)(exprId, qualifier.headOption)
  }

  // scalastyle:on

  override def withNewChild(insert: InsertIntoTable, newChild: LogicalPlan): InsertIntoTable = {
    insert.copy(child = newChild)
  }

  override def newInsertIntoTable(table: LogicalPlan,
      partition: Map[String, Option[String]], child: LogicalPlan,
      overwrite: Boolean, ifNotExists: Boolean): InsertIntoTable = {
    InsertIntoTable(table, partition, child, OverwriteOptions(enabled = overwrite), ifNotExists)
  }

  override def getOverwriteOption(insert: InsertIntoTable): Boolean = insert.overwrite.enabled

  override def newGroupingSet(groupingSets: Seq[Seq[Expression]],
      groupByExprs: Seq[Expression], child: LogicalPlan,
      aggregations: Seq[NamedExpression]): LogicalPlan = {
    val keyMap = groupByExprs.zipWithIndex.toMap
    val numExpressions = keyMap.size
    val mask = (1 << numExpressions) - 1
    val bitmasks: Seq[Int] = groupingSets.map(set => set.foldLeft(mask)((bitmap, col) => {
      if (!keyMap.contains(col)) {
        throw new ParseException(s"GROUPING SETS column '$col' does not appear in GROUP BY list")
      }
      bitmap & ~(1 << (numExpressions - 1 - keyMap(col)))
    }))
    GroupingSets(bitmasks, groupByExprs, child, aggregations)
  }

  override def newSubqueryAlias(alias: String, child: LogicalPlan,
      view: Option[TableIdentifier]): SubqueryAlias = view match {
    case None => child match {
      case u: UnresolvedRelation if u.alias.isEmpty =>
        UnresolvedRelation(u.tableIdentifier, Some(alias))
      case _ => SubqueryAlias(alias, child, view)
    }
    case _ => SubqueryAlias(alias, child, view)
  }

  override def getViewFromAlias(q: SubqueryAlias): Option[TableIdentifier] = q.view

  override def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId, qualifier: Seq[String]): Alias = {
    copyAlias match {
      case None => Alias(child, name)(exprId, qualifier.headOption)
      case Some(a: Alias) =>
        Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata, a.isGenerated)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier, isGenerated = a.isGenerated)
    }
  }

  override def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan = {
    if (outputColumnNames.isEmpty) child
    else {
      throw new ParseException(s"Aliases ($outputColumnNames) for column names " +
          s"of a sub-plan not supported in Spark $version")
    }
  }

  override def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = {
    SortOrder(child, direction, nullOrdering)
  }

  override def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression = {
    RepartitionByExpression(partitionExpressions, child, Some(numPartitions))
  }

  override def newUnresolvedTableValuedFunction(functionName: String,
      functionArgs: Seq[Expression], outputNames: Seq[String]): UnresolvedTableValuedFunction = {
    if (outputNames.nonEmpty) {
      throw new ParseException(s"Aliases ($outputNames) for table value function " +
          s"'$functionName' not supported in Spark $version")
    }
    UnresolvedTableValuedFunction(functionName, functionArgs)
  }

  override def newUnresolvedHaving(predicate: Expression, child: LogicalPlan): LogicalPlan = {
    Filter(predicate, child)
  }

  private def boundaryInt(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): Int = num match {
    case Some(l: Literal) => l.value.toString.toInt
    case _ => throw new ParseException(
      s"Expression ($num) in frame boundary ($boundaryType) not supported in Spark $version")
  }

  override def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): FrameBoundary = {
    boundaryType match {
      case FrameBoundaryType.UnboundedPreceding => UnboundedPreceding
      case FrameBoundaryType.ValuePreceding => ValuePreceding(boundaryInt(boundaryType, num))
      case FrameBoundaryType.CurrentRow => CurrentRow
      case FrameBoundaryType.UnboundedFollowing => UnboundedFollowing
      case FrameBoundaryType.ValueFollowing => ValueFollowing(boundaryInt(boundaryType, num))
    }
  }

  override def newSpecifiedWindowFrame(frameType: FrameType, frameStart: Any,
      frameEnd: Any): SpecifiedWindowFrame = {
    SpecifiedWindowFrame(frameType, frameStart.asInstanceOf[FrameBoundary],
      frameEnd.asInstanceOf[FrameBoundary])
  }

  override def newLogicalPlanWithHints(child: LogicalPlan,
      hints: Map[String, String]): LogicalPlan = {
    new PlanWithHints21(child, newCaseInsensitiveMap(hints))
  }

  override def newUnresolvedHint(name: String, parameters: Seq[Any],
      child: LogicalPlan): LogicalPlan = child

  override def newTableSample(lowerBound: Double, upperBound: Double, withReplacement: Boolean,
      seed: Long, child: LogicalPlan): Sample = {
    Sample(lowerBound, upperBound, withReplacement, seed, child)(isTableSample = true)
  }

  override def isHintPlan(plan: LogicalPlan): Boolean = plan.isInstanceOf[BroadcastHint]

  override def getHints(plan: LogicalPlan): Map[String, String] = plan match {
    case p: PlanWithHints21 => p.allHints
    case _: BroadcastHint =>
      newCaseInsensitiveMap(Map(QueryHint.JoinType.name -> HintName.JoinType_Broadcast.names.head))
    case _ => Map.empty
  }

  override def isBroadcastable(plan: LogicalPlan): Boolean = plan.statistics.isBroadcastable

  override def newOneRowRelation(): LogicalPlan = OneRowRelation

  override def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan = {
    Generate(generator, join = true, outer, qualifier, generatorOutput, child)
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.write(mode, data)
    ds.copy(userSpecifiedSchema = Some(data.schema.asNullable)).resolveRelation()
  }

  override def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation = {
    if (isStreaming) {
      throw new ParseException(s"Streaming relations not supported in Spark $version")
    }
    LogicalRelation(relation, expectedOutputAttributes, catalogTable)
  }

  override def internalCreateDataFrame(session: SparkSession, catalystRows: RDD[InternalRow],
      schema: StructType, isStreaming: Boolean): Dataset[Row] = {
    if (isStreaming) {
      throw new SparkException(s"Streaming datasets not supported in Spark $version")
    }
    session.internalCreateDataFrame(catalystRows, schema)
  }

  override def newRowDataSourceScanExec(fullOutput: Seq[Attribute], requiredColumnsIndex: Seq[Int],
      filters: Seq[Filter], handledFilters: Seq[Filter], rdd: RDD[InternalRow],
      metadata: Map[String, String], relation: BaseRelation,
      tableIdentifier: Option[TableIdentifier]): RowDataSourceScanExec = {
    RowDataSourceScanExec(requiredColumnsIndex.map(fullOutput), rdd, relation,
      UnknownPartitioning(0), metadata, tableIdentifier)
  }

  override def tableIdentifier(scan: DataSourceScanExec): Option[TableIdentifier] = {
    scan.metastoreTableIdentifier
  }

  override def newCodegenSparkFallback(child: SparkPlan,
      session: SnappySession): CodegenSparkFallback = {
    new CodegenSparkFallback21(child, session)
  }

  override def newLogicalDStreamPlan(output: Seq[Attribute], stream: DStream[InternalRow],
      streamingSnappy: SnappyStreamingContext): LogicalDStreamPlan = {
    new LogicalDStreamPlan21(output, stream)(streamingSnappy)
  }

  override def newCatalogDatabase(name: String, description: String,
      locationUri: String, properties: Map[String, String]): CatalogDatabase = {
    CatalogDatabase(name, description, locationUri, properties)
  }

  override def catalogDatabaseLocationURI(database: CatalogDatabase): String = database.locationUri

  // scalastyle:off

  override def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[AnyRef], viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable = {
    if (ignoredProperties.nonEmpty) {
      throw new SparkException(s"ignoredProperties should be always empty in Spark $version")
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, properties,
      stats.asInstanceOf[Option[Statistics]], viewOriginalText, viewText, comment,
      unsupportedFeatures, tracksPartitionsInCatalog, schemaPreservesCase)
  }

  // scalastyle:on

  override def catalogTableViewOriginalText(catalogTable: CatalogTable): Option[String] =
    catalogTable.viewOriginalText

  override def catalogTableIgnoredProperties(catalogTable: CatalogTable): Map[String, String] =
    Map.empty

  override def newCatalogTableWithViewOriginalText(catalogTable: CatalogTable,
      viewOriginalText: Option[String]): CatalogTable = {
    catalogTable.copy(viewOriginalText = viewOriginalText)
  }

  override def newCatalogStorageFormat(locationUri: Option[String], inputFormat: Option[String],
      outputFormat: Option[String], serde: Option[String], compressed: Boolean,
      properties: Map[String, String]): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serde, compressed, properties)
  }

  override def catalogStorageFormatLocationUri(
      storageFormat: CatalogStorageFormat): Option[String] = storageFormat.locationUri

  override def catalogTablePartitionToRow(partition: CatalogTablePartition,
      partitionSchema: StructType, defaultTimeZoneId: String): InternalRow = {
    partition.toRow(partitionSchema)
  }

  override def loadDynamicPartitions(externalCatalog: ExternalCatalog, db: String,
      table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean,
      numDP: Int, holdDDLTime: Boolean): Unit = {
    externalCatalog.loadDynamicPartitions(db, table, loadPath, partition, replace,
      numDP, holdDDLTime)
  }

  override def alterTableSchema(externalCatalog: ExternalCatalog, dbName: String,
      table: String, newSchema: StructType): Unit = {
    externalCatalog.alterTableSchema(dbName, table, newSchema)
  }

  override def alterTableStats(externalCatalog: ExternalCatalog, db: String, table: String,
      stats: Option[AnyRef]): Unit = {
    throw new ParseException(s"ALTER TABLE STATS not supported in Spark $version")
  }

  override def alterFunction(externalCatalog: ExternalCatalog, db: String,
      function: CatalogFunction): Unit = {
    throw new ParseException(s"ALTER FUNCTION not supported in Spark $version")
  }

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[ColumnStat].toMap
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[AnyRef] = {
    ColumnStat.fromMap(table, field, map)
  }

  override def toCatalogStatistics(sizeInBytes: BigInt, rowCount: Option[BigInt],
      colStats: Map[String, AnyRef]): AnyRef = {
    Statistics(sizeInBytes, rowCount, colStats.asInstanceOf[Map[String, ColumnStat]])
  }

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog21(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog = {
    new SmartConnectorExternalCatalog21(session)
  }

  override protected def basicLookupDataSource(provider: String, conf: => SQLConf): Class[_] =
    DataSource.lookupDataSource(provider)

  override def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    ShuffleExchange(newPartitioning, child)
  }

  override def isShuffleExchange(plan: SparkPlan): Boolean = plan.isInstanceOf[ShuffleExchange]

  override def classOfShuffleExchange(): Class[_] = classOf[ShuffleExchange]

  override def getStatistics(plan: LogicalPlan): Statistics = plan.statistics

  override def supportsPartial(aggregate: AggregateFunction): Boolean = aggregate.supportsPartial

  override def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression], resultExpressions: Seq[NamedExpression],
      planChild: () => SparkPlan): Seq[SparkPlan] = {
    aggregate.AggUtils.planAggregateWithoutPartial(
      groupingExpressions,
      aggregateExpressions,
      resultExpressions,
      planChild())
  }

  override def compile(code: CodeAndComment): GeneratedClass = CodeGenerator.compile(code)

  override def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions = new JSONOptions(parameters)

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionState21(snappySession)
  }

  override def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit = {
    // we pass wrapper catalog to make sure LogicalRelation
    // is passed in PreWriteCheck
    PreWriteCheck(sessionState.conf, sessionState.wrapperCatalog)
  }

  override def hiveConditionalStrategies(sessionState: SnappySessionState): Seq[Strategy] = {
    new HiveConditionalStrategy(_.HiveTableScans, sessionState) ::
        new HiveConditionalStrategy(_.DataSinks, sessionState) ::
        new HiveConditionalStrategy(_.Scripts, sessionState) :: Nil
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager21

  override def buildConf(key: String): ConfigBuilder = SQLConfigBuilder(key)

  override def getCachedRDDInfos(context: SparkContext): Seq[RDDStorageInfo] = {
    context.ui.get.storageListener.rddInfoList.map(info => new RDDStorageInfo(info.id, info.name,
      info.numPartitions, info.numCachedPartitions, info.storageLevel.description,
      info.memSize, info.diskSize, dataDistribution = None, partitions = None))
  }

  override def getReturnDataType(method: Method): DataType = {
    HiveAccessUtil.javaClassToDataType(method.getReturnType)
  }

  override def newExprCode(code: String, isNull: String, value: String, dt: DataType): ExprCode = {
    ExprCode(code, isNull, value)
  }

  override def copyExprCode(ev: ExprCode, code: String, isNull: String,
      value: String, dt: DataType): ExprCode = {
    ev.copy(code = if (code ne null) code else ev.code,
      isNull = if (isNull ne null) isNull else ev.isNull,
      value = if (value ne null) value else ev.value)
  }

  override def resetCode(ev: ExprCode): Unit = {
    ev.code = ""
  }

  override def exprCodeIsNull(ev: ExprCode): String = ev.isNull

  override def setExprCodeIsNull(ev: ExprCode, isNull: String): Unit = {
    ev.isNull = isNull
  }

  override def exprCodeValue(ev: ExprCode): String = ev.value

  override def javaType(dt: DataType, ctx: CodegenContext): String = ctx.javaType(dt)

  override def boxedType(javaType: String, ctx: CodegenContext): String = ctx.boxedType(javaType)

  override def defaultValue(dt: DataType, ctx: CodegenContext): String = ctx.defaultValue(dt)

  override def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean = {
    ctx.isPrimitiveType(javaType)
  }

  override def primitiveTypeName(javaType: String, ctx: CodegenContext): String = {
    ctx.primitiveTypeName(javaType)
  }

  override def getValue(input: String, dataType: DataType, ordinal: String,
      ctx: CodegenContext): String = {
    ctx.getValue(input, dataType, ordinal)
  }

  override def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]] = {
    python.ExtractPythonUDFs :: Nil
  }

  override def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot = {
    if (!pivotValues.forall(_.isInstanceOf[Literal])) {
      throw new AnalysisException(
        s"Literal expressions required for pivot values, found: ${pivotValues.mkString("; ")}")
    }
    Pivot(groupByExprs, pivotColumn, pivotValues.map(_.asInstanceOf[Literal]), aggregates, child)
  }

  override def copyPivot(pivot: Pivot, groupByExprs: Seq[NamedExpression]): Pivot = {
    pivot.copy(groupByExprs = groupByExprs)
  }

  override def newIntersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Intersect = {
    if (isAll) {
      throw new ParseException(s"INTERSECT ALL not supported in spark $version")
    }
    Intersect(left, right)
  }

  override def newExcept(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Except = {
    if (isAll) {
      throw new ParseException(s"EXCEPT ALL not supported in spark $version")
    }
    Except(left, right)
  }

  override def newExplainCommand(logicalPlan: LogicalPlan, extended: Boolean,
      codegen: Boolean, cost: Boolean): LogicalPlan = {
    if (cost) {
      throw new ParseException(s"EXPLAIN COST not supported in spark $version")
    }
    ExplainCommand(logicalPlan, extended = extended, codegen = codegen)
  }

  override def newTruncateTableCommand(tableName: TableIdentifier): Option[RunnableCommand] = None

  override def cachedColumnBuffers(relation: InMemoryRelation): RDD[_] = {
    relation.cachedColumnBuffers
  }

  override def addStringPromotionRules(rules: Seq[Rule[LogicalPlan]],
      analyzer: SnappyAnalyzer, conf: SQLConf): Seq[Rule[LogicalPlan]] = {
    rules.flatMap {
      case PromoteStrings =>
        (analyzer.StringPromotionCheckForUpdate :: analyzer.SnappyPromoteStrings ::
            PromoteStrings :: Nil).asInstanceOf[Seq[Rule[LogicalPlan]]]
      case r => r :: Nil
    }
  }

  override def newHashClusteredDistribution(expressions: Seq[Expression],
      requiredNumPartitions: Option[Int]): Distribution = {
    ClusteredDistribution(expressions, requiredNumPartitions)
  }

  override def newClusteredPartitioning(distribution: Distribution,
      numPartitions: Int): Partitioning = {
    HashPartitioning(distribution.asInstanceOf[ClusteredDistribution].clustering, numPartitions)
  }
}

/**
 * Simple extension to CacheManager to enable clearing cached plans on cache create/drop.
 */
class SnappyCacheManager21 extends CacheManager {

  override def cacheQuery(query: Dataset[_], tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    super.cacheQuery(query, tableName, storageLevel)
    // clear plan cache since cached representation can change existing plans
    query.sparkSession.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPlan(session: SparkSession, plan: LogicalPlan): Unit = {
    super.recacheByPlan(session, plan)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPath(session: SparkSession, resourcePath: String): Unit = {
    super.recacheByPath(session, resourcePath)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}

class SnappyEmbeddedHiveCatalog21(_conf: SparkConf, _hadoopConf: Configuration,
    _createTime: Long) extends SnappyHiveExternalCatalog(_conf, _hadoopConf, _createTime) {

  override def getTable(db: String, table: String): CatalogTable =
    getTableImpl(db, table)

  override def getTableOption(db: String, table: String): Option[CatalogTable] =
    getTableIfExists(db, table)

  override protected def baseCreateDatabase(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = super.createDatabase(dbDefinition, ignoreIfExists)

  override protected def baseDropDatabase(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = super.dropDatabase(db, ignoreIfNotExists, cascade)

  override protected def baseCreateTable(tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = super.createTable(tableDefinition, ignoreIfExists)

  override protected def baseDropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = super.dropTable(db, table, ignoreIfNotExists, purge)

  override protected def baseAlterTable(tableDefinition: CatalogTable): Unit =
    super.alterTable(tableDefinition)

  override protected def baseRenameTable(db: String, oldName: String, newName: String): Unit =
    super.renameTable(db, oldName, newName)

  override protected def baseLoadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    super.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override protected def baseCreateFunction(db: String,
      funcDefinition: CatalogFunction): Unit = super.createFunction(db, funcDefinition)

  override protected def baseDropFunction(db: String, name: String): Unit =
    super.dropFunction(db, name)

  override protected def baseRenameFunction(db: String, oldName: String,
      newName: String): Unit = super.renameFunction(db, oldName, newName)

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    createDatabaseImpl(dbDefinition, ignoreIfExists)

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    dropDatabaseImpl(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(dbDefinition)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    dropTableImpl(db, table, ignoreIfNotExists, purge)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit =
    renameTableImpl(db, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def loadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    loadDynamicPartitionsImpl(db, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override def listPartitionsByFilter(db: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitionsByFilter(db, table, predicates))
  }

  override def createFunction(db: String, function: CatalogFunction): Unit =
    createFunctionImpl(db, function)

  override def dropFunction(db: String, funcName: String): Unit =
    dropFunctionImpl(db, funcName)

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(db, oldName, newName)
}

class SmartConnectorExternalCatalog21(override val session: SparkSession)
    extends SmartConnectorExternalCatalog {

  override def getTable(db: String, table: String): CatalogTable =
    getTableImpl(db, table)

  override def getTableOption(db: String, table: String): Option[CatalogTable] =
    getTableIfExists(db, table)

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    createDatabaseImpl(dbDefinition, ignoreIfExists)

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    dropDatabaseImpl(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    throw new UnsupportedOperationException("Database definitions cannot be altered")

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    dropTableImpl(db, table, ignoreIfNotExists, purge)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit =
    renameTableImpl(db, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def alterTableSchema(dbName: String, table: String, newSchema: StructType): Unit =
    alterTableSchemaImpl(dbName, table, newSchema)

  override def loadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    loadDynamicPartitionsImpl(db, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override def listPartitionsByFilter(db: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    listPartitionsByFilterImpl(db, table, predicates, defaultTimeZoneId = "")
  }

  override def createFunction(db: String, function: CatalogFunction): Unit =
    createFunctionImpl(db, function)

  override def dropFunction(db: String, funcName: String): Unit =
    dropFunctionImpl(db, funcName)

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(db, oldName, newName)
}

class SnappySessionCatalog21(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SessionCatalog(snappyExternalCatalog, globalTempManager, functionResourceLoader,
      functionRegistry, sqlConf, hadoopConf) with SnappySessionCatalog {

  override def functionNotFound(name: String): Nothing = {
    super.failFunctionLookup(name)
  }

  override protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = super.createTable(table, ignoreIfExists)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    createTableImpl(table, ignoreIfExists, validateTableLocation = true)
  }

  override def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    super.getTableMetadataOption(name) match {
      case None => None
      case Some(table) => Some(convertCharTypes(table))
    }
  }

  override def newView(table: CatalogTable, child: LogicalPlan): LogicalPlan = child

  override def newCatalogRelation(dbName: String, table: CatalogTable): LogicalPlan =
    SimpleCatalogRelation(dbName, table)

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan =
    lookupRelationImpl(name, alias)

  override def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder =
    makeFunctionBuilderImpl(name, functionClassName)
}

class SnappySessionState21(override val snappySession: SnappySession)
    extends SessionState(snappySession) with SnappySessionState {

  self =>

  override def catalogBuilder(wrapped: Option[SnappySessionCatalog]): SnappySessionCatalog = {
    new SnappySessionCatalog21(snappySession,
      snappySession.sharedState.getExternalCatalogInstance(snappySession),
      snappySession.sharedState.globalTempViewManager,
      functionResourceLoader, functionRegistry, sqlParser, conf, newHadoopConf(), wrapped)
  }

  override def analyzerBuilder(): Analyzer = new SnappyAnalyzer(catalog, conf) {

    self =>

    override def session: SnappySession = snappySession

    private def state: SnappySessionState = session.snappySessionState

    private def hiveCatalog(state: SessionState): HiveSessionCatalog =
      state.catalog.asInstanceOf[HiveSessionCatalog]

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = {
      val extensions1 = session.contextFunctions.getExtendedResolutionRules
      val extensions2 = session.contextFunctions.getPostHocResolutionRules
      val rules = new HiveConditionalRule(hiveCatalog(_).ParquetConversions, state) ::
          new HiveConditionalRule(hiveCatalog(_).OrcConversions, state) ::
          AnalyzeCreateTable(session) ::
          new PreprocessTable(state) ::
          ResolveAliasInGroupBy ::
          new FindDataSourceTable(session) ::
          ResolveInsertIntoPlan ::
          DataSourceAnalysis(conf) ::
          AnalyzeMutableOperations(session, this) ::
          ResolveQueryHints(session) ::
          RowLevelSecurity ::
          ExternalRelationLimitFetch ::
          (if (conf.runSQLonFile) new ResolveDataSource(session) :: extensions2 else extensions2)
      if (extensions1.isEmpty) rules else extensions1 ++ rules
    }

    override val extendedCheckRules: Seq[LogicalPlan => Unit] = getExtendedCheckRules
  }

  override def optimizerBuilder(): Optimizer = {
    new SparkOptimizer(catalog, conf, experimentalMethods) with DefaultOptimizer {

      override def state: SnappySessionState = self

      override def batches: Seq[Batch] = batchesImpl
    }
  }

  override lazy val conf: SQLConf = new SnappyConf(snappySession)

  override lazy val sqlParser: SnappySqlParser = snappySession.contextFunctions.newSQLParser()

  override lazy val streamingQueryManager: SnappyStreamingQueryManager = {
    new SnappyStreamingQueryManager(snappySession) {

      override private[sql] def startQuery(
          userSpecifiedName: Option[String],
          userSpecifiedCheckpointLocation: Option[String],
          df: DataFrame,
          sink: Sink,
          outputMode: OutputMode,
          useTempCheckpointLocation: Boolean,
          recoverFromCheckpointLocation: Boolean,
          trigger: Trigger,
          triggerClock: Clock): StreamingQuery = {
        checkStartQuery(sink)
        super.startQuery(userSpecifiedName, userSpecifiedCheckpointLocation, df, sink, outputMode,
          useTempCheckpointLocation, recoverFromCheckpointLocation, trigger, triggerClock)
      }
    }
  }
}

class CodegenSparkFallback21(child: SparkPlan,
    session: SnappySession) extends CodegenSparkFallback(child, session) {

  override def generateTreeString(depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder,
      verbose: Boolean, prefix: String): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, prefix)
  }
}

class LogicalDStreamPlan21(output: Seq[Attribute],
    stream: DStream[InternalRow])(streamingSnappy: SnappyStreamingContext)
    extends LogicalDStreamPlan(output, stream)(streamingSnappy) {

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = BigInt(streamingSnappy.snappySession.sessionState.conf.defaultSizeInBytes)
  )
}
