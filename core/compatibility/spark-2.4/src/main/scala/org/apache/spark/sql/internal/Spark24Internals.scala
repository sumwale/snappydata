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

import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCoercion, UnresolvedAttribute, UnresolvedHaving}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, LambdaFunction, NamedExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Except, Intersect, LogicalPlan, Pivot, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.{CacheManager, SparkOptimizer, SparkPlan}
import org.apache.spark.sql.hive.{HiveSessionResourceLoader, SnappyAnalyzer, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{BooleanType, DataType, Metadata, StructField, StructType}
import org.apache.spark.ui.SparkUI
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Base implementation of [[SparkInternals]] for Spark 2.4.x releases.
 */
class Spark24Internals(override val version: String) extends Spark23_4_Internals {

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade, blocking)
  }

  override def toAttributeReference(attr: Attribute)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata,
      exprId: ExprId): AttributeReference = {
    AttributeReference(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId, qualifier = attr.qualifier)
  }

  override def newLambdaFunction(expression: Expression, args: Seq[Expression]): Expression = {
    val arguments = args.map {
      case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
      case ne: NamedExpression => ne.transformUp {
        case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
      }
      case e =>
        throw new ParseException(s"Lambda argument should be named expression: ${e.treeString}")
    }.asInstanceOf[Seq[NamedExpression]]
    val function = expression.transformUp {
      case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
    }
    LambdaFunction(function, arguments)
  }

  override def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String],
      isGenerated: Boolean): AttributeReference = {
    AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
  }

  override def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, realExprId: ExprId, exprId: ExprId,
      qualifier: Seq[String]): ErrorEstimateAttribute = {
    ErrorEstimateAttribute24(name, dataType, nullable, metadata, realExprId)(exprId, qualifier)
  }

  override def newApproxColumnExtractor(child: Expression, name: String, ordinal: Int,
      dataType: DataType, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ApproxColumnExtractor = {
    ApproxColumnExtractor24(child, name, ordinal, dataType, nullable)(exprId, qualifier)
  }

  override def newTaggedAttribute(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String]): TaggedAttribute = {
    TaggedAttribute24(tag, name, dataType, nullable, metadata)(exprId, qualifier)
  }

  override def newTaggedAlias(tag: TransformableTag, child: Expression, name: String,
      exprId: ExprId, qualifier: Seq[String]): TaggedAlias = {
    TaggedAlias24(tag, child, name)(exprId, qualifier)
  }

  // scalastyle:off

  override def newClosedFormColumnExtractor(child: Expression, name: String, confidence: Double,
      confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
      behavior: HAC.Type, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ClosedFormColumnExtractor = {
    ClosedFormColumnExtractor24(child, name, confidence, confFactor, aggType, error,
      dataType, behavior, nullable)(exprId, qualifier)
  }

  // scalastyle:on

  override def newSubqueryAlias(alias: String, child: LogicalPlan,
      view: Option[TableIdentifier]): LogicalPlan = view match {
    case Some(v@TableIdentifier(table, dbOpt)) =>
      if (!alias.equalsIgnoreCase(table)) {
        throw new AnalysisException(s"Conflicting alias and view: alias=$alias, view=$v")
      } else {
        SubqueryAlias(AliasIdentifier(table, dbOpt), child)
      }
    case _ => SubqueryAlias(AliasIdentifier(alias, None), child)
  }

  override def getViewFromAlias(q: SubqueryAlias): Option[TableIdentifier] = q.name match {
    case AliasIdentifier(_, None) => None
    case AliasIdentifier(id, db) => Some(TableIdentifier(id, db))
  }

  override def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId, qualifier: Seq[String]): Alias = {
    copyAlias match {
      case None => Alias(child, name)(exprId, qualifier)
      case Some(a: Alias) => Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier)
    }
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.writeAndRead(mode, data.logicalPlan, data.logicalPlan.output.map(_.name),
      data.queryExecution.executedPlan)
  }

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[CatalogColumnStat].toMap(colName)
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[AnyRef] = {
    CatalogColumnStat.fromMap(table, field.name, map)
  }

  override def toCatalogStatistics(sizeInBytes: BigInt, rowCount: Option[BigInt],
      colStats: Map[String, AnyRef]): AnyRef = {
    CatalogStatistics(sizeInBytes, rowCount, colStats.asInstanceOf[Map[String, CatalogColumnStat]])
  }

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog24(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog = {
    new SmartConnectorExternalCatalog24(session)
  }

  override def detachHandler(ui: SparkUI, path: String): Unit = {
    ui.detachHandler(path)
  }

  override def newSharedState(sparkContext: SparkContext): SnappySharedState = {
    // remove any existing SQLTab since a new one will be created by SharedState constructor
    removeSQLTabs(sparkContext, except = None)
    val state = new SnappySharedState24(sparkContext)
    createAndAttachSQLListener(state, sparkContext)
    state
  }

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionStateBuilder24(snappySession, snappySession.cloneSessionState).build()
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager24

  private def exprValue(v: String, dt: DataType): ExprValue = v match {
    case "false" => FalseLiteral
    case "true" => TrueLiteral
    case _ if v.indexOf(' ') != -1 => SimpleExprValue(v, CodeGenerator.javaClass(dt))
    case _ => VariableValue(v, CodeGenerator.javaClass(dt))
  }

  override def newExprCode(code: String, isNull: String,
      value: String, dt: DataType): ExprCode = {
    ExprCode(if (code.isEmpty) EmptyBlock else CodeBlock(code :: Nil, EmptyBlock :: Nil),
      isNull = exprValue(isNull, BooleanType),
      value = exprValue(value, dt))
  }

  override def copyExprCode(ev: ExprCode, code: String, isNull: String,
      value: String, dt: DataType): ExprCode = {
    val codeBlock =
      if (code eq null) ev.code
      else if (code.isEmpty) EmptyBlock
      else CodeBlock(code :: Nil, EmptyBlock :: Nil)
    ev.copy(codeBlock,
      isNull = if (isNull ne null) exprValue(isNull, BooleanType) else ev.isNull,
      value = if (value ne null) exprValue(value, dt) else ev.value)
  }

  override def resetCode(ev: ExprCode): Unit = {
    ev.code = EmptyBlock
  }

  override def exprCodeIsNull(ev: ExprCode): String = ev.isNull.code

  override def setExprCodeIsNull(ev: ExprCode, isNull: String): Unit = {
    ev.isNull = exprValue(isNull, BooleanType)
  }

  override def exprCodeValue(ev: ExprCode): String = ev.value.code

  override def javaType(dt: DataType, ctx: CodegenContext): String = CodeGenerator.javaType(dt)

  override def boxedType(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.boxedType(javaType)
  }

  override def defaultValue(dt: DataType, ctx: CodegenContext): String = {
    CodeGenerator.defaultValue(dt)
  }

  override def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean = {
    CodeGenerator.isPrimitiveType(javaType)
  }

  override def primitiveTypeName(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.primitiveTypeName(javaType)
  }

  override def getValue(input: String, dataType: DataType, ordinal: String,
      ctx: CodegenContext): String = {
    CodeGenerator.getValue(input, dataType, ordinal)
  }

  override def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]] = Nil

  override def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot = {
    Pivot(if (groupByExprs.isEmpty) None else Some(groupByExprs), pivotColumn, pivotValues,
      aggregates, child)
  }

  override def copyPivot(pivot: Pivot, groupByExprs: Seq[NamedExpression]): Pivot = {
    pivot.copy(groupByExprsOpt = if (groupByExprs.isEmpty) None else Some(groupByExprs))
  }

  override def newIntersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Intersect = {
    Intersect(left, right, isAll)
  }

  override def newExcept(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Except = {
    Except(left, right, isAll)
  }

  override def newUnresolvedHaving(predicate: Expression, child: LogicalPlan): LogicalPlan = {
    UnresolvedHaving(predicate, child)
  }

  override def cachedColumnBuffers(relation: InMemoryRelation): RDD[_] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  override def addStringPromotionRules(rules: Seq[Rule[LogicalPlan]],
      analyzer: SnappyAnalyzer, conf: SQLConf): Seq[Rule[LogicalPlan]] = {
    rules.flatMap {
      case _: TypeCoercion.PromoteStrings =>
        (analyzer.StringPromotionCheckForUpdate :: analyzer.SnappyPromoteStrings ::
            TypeCoercion.PromoteStrings(conf) :: Nil).asInstanceOf[Seq[Rule[LogicalPlan]]]
      case r => r :: Nil
    }
  }

  override def createTable(catalog: SessionCatalog, tableDefinition: CatalogTable,
      ignoreIfExists: Boolean, validateLocation: Boolean): Unit = {
    catalog.createTable(tableDefinition, ignoreIfExists, validateLocation)
  }

  override def logicalPlanResolveDown(plan: LogicalPlan)(
      rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    plan.resolveOperatorsDown(rule)
  }

  override def logicalPlanResolveUp(plan: LogicalPlan)(
      rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    plan.resolveOperatorsUp(rule)
  }

  override def logicalPlanResolveExpressions(plan: LogicalPlan)(
      rule: PartialFunction[Expression, Expression]): LogicalPlan = {
    plan.resolveExpressions(rule)
  }
}

class SnappyEmbeddedHiveCatalog24(_conf: SparkConf, _hadoopConf: Configuration,
    _createTime: Long) extends SnappyHiveExternalCatalog(_conf, _hadoopConf, _createTime) {

  override def getTable(db: String, table: String): CatalogTable =
    getTableImpl(db, table)

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
    super.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP)
  }

  override protected def baseCreateFunction(db: String,
      funcDefinition: CatalogFunction): Unit = super.createFunction(db, funcDefinition)

  override protected def baseDropFunction(db: String, name: String): Unit =
    super.dropFunction(db, name)

  override protected def baseRenameFunction(db: String, oldName: String,
      newName: String): Unit = super.renameFunction(db, oldName, newName)

  override def createDatabase(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(dbDefinition, ignoreIfExists)

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(dbDefinition)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(db, table, ignoreIfNotExists, purge)

  override def renameTable(db: String, oldName: String, newName: String): Unit =
    renameTableImpl(db, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def alterTableStats(db: String, table: String,
      stats: Option[CatalogStatistics]): Unit = {
    withHiveExceptionHandling(super.alterTableStats(db, table, stats))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def alterTableDataSchema(db: String, table: String, newSchema: StructType): Unit = {
    withHiveExceptionHandling(super.alterTableDataSchema(db, table, newSchema))

    registerCatalogSchemaChange(db -> table :: Nil)
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = {
    loadDynamicPartitionsImpl(db, table, loadPath, partition, replace, numDP,
      holdDDLTime = false)
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitionsByFilter(db, table,
      predicates, defaultTimeZoneId))
  }

  override def createFunction(db: String, function: CatalogFunction): Unit =
    createFunctionImpl(db, function)

  override def dropFunction(db: String, funcName: String): Unit =
    dropFunctionImpl(db, funcName)

  override def alterFunction(db: String, function: CatalogFunction): Unit = {
    withHiveExceptionHandling(super.alterFunction(db, function))
    SnappySession.clearAllCache()
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(db, oldName, newName)
}

class SmartConnectorExternalCatalog24(override val session: SparkSession)
    extends SmartConnectorExternalCatalog {

  override def getTable(db: String, table: String): CatalogTable =
    getTableImpl(db, table)

  override def createDatabase(dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(dbDefinition, ignoreIfExists)

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    throw new UnsupportedOperationException("Database definitions cannot be altered")

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(db, table, ignoreIfNotExists, purge)

  override def renameTable(db: String, oldName: String, newName: String): Unit =
    renameTableImpl(db, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def alterTableDataSchema(dbName: String, table: String,
      newSchema: StructType): Unit = alterTableSchemaImpl(dbName, table, newSchema)

  override def alterTableStats(db: String, table: String,
      stats: Option[CatalogStatistics]): Unit = stats match {
    case None => alterTableStatsImpl(db, table, None)
    case Some(s) => alterTableStatsImpl(db, table,
      Some((s.sizeInBytes, s.rowCount, s.colStats)))
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = {
    loadDynamicPartitionsImpl(db, table, loadPath, partition, replace, numDP,
      holdDDLTime = false)
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    listPartitionsByFilterImpl(db, table, predicates, defaultTimeZoneId)
  }

  override def createFunction(db: String, function: CatalogFunction): Unit =
    createFunctionImpl(db, function)

  override def dropFunction(db: String, funcName: String): Unit =
    dropFunctionImpl(db, funcName)

  override def alterFunction(db: String, function: CatalogFunction): Unit =
    alterFunctionImpl(db, function)

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(db, oldName, newName)
}

class SnappySessionCatalog24(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SessionCatalog(() => snappyExternalCatalog,
      () => snappySession.sharedState.globalTempViewManager, functionRegistry, sqlConf,
      hadoopConf, parser, functionResourceLoader) with SnappySessionCatalog23_4 {

  override def globalTempManager: GlobalTempViewManager = globalTempViewManager

  override protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    super.createTable(table, ignoreIfExists, validateTableLocation)
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    createTableImpl(table, ignoreIfExists, validateTableLocation)
  }
}

class SnappySessionStateBuilder24(session: SnappySession, parentState: Option[SessionState] = None)
    extends SnappySessionStateBuilder23_4(session, parentState) {

  override protected lazy val resourceLoader: SessionResourceLoader = externalCatalog match {
    case c: SnappyHiveExternalCatalog => new HiveSessionResourceLoader(session, c.client)
    case _ => new SessionResourceLoader(session)
  }

  override protected def newSessionCatalog(
      wrapped: Option[SnappySessionCatalog]): SnappySessionCatalog = {
    new SnappySessionCatalog24(
      session,
      externalCatalog,
      resourceLoader,
      functionRegistry,
      sqlParser,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      wrapped)
  }

  override protected def optimizer: Optimizer = {
    new SparkOptimizer(catalog, experimentalMethods) with DefaultOptimizer {

      private[this] var depth = 0

      override def state: SnappySessionState = session.snappySessionState

      override def defaultBatches: Seq[Batch] = {
        if (depth == 0) {
          depth += 1
          try {
            batchesImpl
          } finally {
            depth -= 1
          }
        } else super.defaultBatches
      }

      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }

  override protected def newBuilder: NewBuilder = (session, optState) =>
    new SnappySessionStateBuilder24(session.asInstanceOf[SnappySession], optState)
}

/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
class SnappyCacheManager24 extends SnappyCacheManager23_4 {

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, cascade, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}
