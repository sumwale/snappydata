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
package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.LocalRegion
import io.snappydata.Property

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, _}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnarStorePartitionedRDD, IndexColumnFormatRelation, SmartConnectorColumnRDD}
import org.apache.spark.sql.execution.columnar.{ColumnInsertExec, ColumnPutIntoExec, ConnectionType}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricInfo, SQLMetrics}
import org.apache.spark.sql.execution.row.{RowFormatRelation, RowFormatScanRDD, RowInsertExec}
import org.apache.spark.sql.sources.{BaseRelation, PrunedUnsafeFilteredScan, SamplingRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, CachedDataFrame, SnappySession, SparkSupport}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Physical plan node for scanning data from an DataSource scan RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes this SparkPla can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * make it inline with the partitioning of the underlying DataSource
 */
private[sql] abstract class PartitionedPhysicalScan(
    output: Seq[Attribute],
    dataRDD: RDD[Any],
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    @transient val relation: BaseRelation)
    extends LeafExecNode with CodegenSupportOnExecutor
        with NonRecursivePlans with SparkSupport {

  def getMetrics: Map[String, SQLMetric] = {
    if (sqlContext eq null) Map.empty
    else Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))
  }

  override lazy val metrics: Map[String, SQLMetric] = getMetrics

  private lazy val extraInformation = if (relation != null) {
    relation.toString
  } else {
    "<extraInformation:NULL>"
  }

  protected lazy val numPartitions: Int = if (dataRDD != null) {
    dataRDD.getNumPartitions
  } else {
    -1
  }

  def caseSensitive: Boolean

  @transient val (metricAdd, metricValue): (String => String, String => String) =
    Utils.metricMethods

  // RDD cast as RDD[InternalRow] below just to satisfy interfaces like
  // inputRDDs though its actually of CachedBatches, CompactExecRows, etc
  val rdd: RDD[InternalRow] = dataRDD.asInstanceOf[RDD[InternalRow]]

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    // when buckets are linked to partitions then actual buckets needs to be considered.
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val linkPart = session.hasLinkPartitionsToBuckets || session.preferPrimaries
    // The SinglePartition here is an optimization that can avoid an Exchange for the case
    // of simple queries. This partitioning is compatible with all other required
    // distributions though the table is still HashPartitioned. This helps for the
    // case of aggregation with limit, for example, (SNAP-) which cannot be converted
    // to use CollectAggregateExec. For cases where HashPartitioning of the table does
    // require to be repartitioned due to a sub-query/join, "linkPart" will be true
    // so it will fall into HashPartitioning.
    if (numPartitions == 1 && (numBuckets == 1 || !linkPart)) {
      SinglePartition
    } else if (partitionColumns.nonEmpty) {
      HashPartitioning(partitionColumns, if (linkPart) numBuckets else numPartitions)
    } else super.outputPartitioning
  }

  override lazy val simpleString: String = {
    val s = "Partitioned Scan " + extraInformation +
        ", Requested Columns = " + output.mkString("[", ",", "]") +
        " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
        " numBuckets = " + numBuckets + " numPartitions = " + numPartitions)
    /* TODO: doesn't work because this simpleString is not re-evaluated (even if made def)
     * also will need to handle the possible case where numPartitions can change in future
    if (numPartitions == 1 && numBuckets > 1) {
      val partitionStr = dataRDD.partitions(0) match {
        case z: ZippedPartitionsPartition => z.partitions(1).toString
        case p => p.toString
      }
      s += " prunedPartition = " + partitionStr
    } else {
      s += " numPartitions = " + numPartitions
    }
    */
    val rdd = dataRDD match {
      // column scan will create zip of 2 partitions with second being the column one
      case z: ZippedPartitionsBaseRDD[_] => z.rdds(1)
      case r => r
    }
    val filters = rdd match {
      case c: ColumnarStorePartitionedRDD => c.filters
      case r: RowFormatScanRDD => r.filters
      case s: SmartConnectorColumnRDD => s.filters
      case _ => Array.empty[Expression]
    }
    if (filters != null && filters.length > 0) filters.mkString(s + " filters = ", ",", "") else s
  }
}

private[sql] object PartitionedPhysicalScan extends SparkSupport {

  def createFromDataSource(
      output: Seq[Attribute],
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      partitionColumnAliases: Seq[Seq[Attribute]],
      rdd: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]],
      relation: PartitionedDataSourceScan,
      allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference],
      scanBuilderArgs: => (Seq[AttributeReference], Seq[Expression])): SparkPlan =
    relation match {
      case i: IndexColumnFormatRelation =>
        val caseSensitive = i.sqlContext.conf.caseSensitiveAnalysis
        val columnScan = internals.columnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, partitionColumnAliases, relation, relation.schema,
          allFilters, schemaAttributes, caseSensitive)
        val table = i.getBaseTableRelation
        val (a, f) = scanBuilderArgs
        val baseTableRDD = table.buildRowBufferRDD(() => Array.empty,
          a.map(_.name).toArray, f.toArray, useResultSet = false, projection = null)

        def resolveCol(left: Attribute, right: AttributeReference) =
          columnScan.sqlContext.sessionState.analyzer.resolver(left.name, right.name)

        val rowBufferScan = internals.rowTableScan(output, StructType.fromAttributes(
          output), baseTableRDD, numBuckets, Nil, Nil, table.table, table, caseSensitive)
        val otherPartKeys = partitionColumns.map(_.transform {
          case a: AttributeReference => rowBufferScan.output.find(resolveCol(_, a)).getOrElse {
            throw new AnalysisException(s"RowBuffer output column $a not found in " +
                s"${rowBufferScan.output.mkString(",")}")
          }
        })
        assert(columnScan.outputPartitioning.satisfies(
          internals.newHashClusteredDistribution(columnScan.partitionColumns)))
        ZipPartitionScan(columnScan, columnScan.partitionColumns,
          rowBufferScan, otherPartKeys)
      case c: BaseColumnFormatRelation =>
        internals.columnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, partitionColumnAliases, relation, relation.schema,
          allFilters, schemaAttributes, c.sqlContext.conf.caseSensitiveAnalysis)
      case r: SamplingRelation =>
        if (r.isReservoirAsRegion) {
          internals.columnTableScan(output, rdd, Nil, numBuckets, partitionColumns,
            partitionColumnAliases, relation, relation.schema, allFilters,
            schemaAttributes, r.sqlContext.conf.caseSensitiveAnalysis,
            isSampleReservoirAsRegion = true)
        } else {
          internals.columnTableScan(output, rdd, otherRDDs, numBuckets,
            partitionColumns, partitionColumnAliases, relation, relation.schema,
            allFilters, schemaAttributes, r.sqlContext.conf.caseSensitiveAnalysis)
        }
      case r: RowFormatRelation =>
        internals.rowTableScan(output, StructType.fromAttributes(output), rdd, numBuckets,
          partitionColumns, partitionColumnAliases, relation.table, relation,
          r.sqlContext.conf.caseSensitiveAnalysis)
    }

  def getSparkPlanInfo(fullPlan: SparkPlan,
      paramLiterals: Array[ParamLiteral] = SnappySession.EMPTY_PARAMS,
      paramsId: Int = -1): SparkPlanInfo = {
    val plan = fullPlan match {
      case CodegenSparkFallback(child, _) => child
      case _ => fullPlan
    }
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    val simpleString = SnappySession.replaceParamLiterals(
      plan.simpleString, paramLiterals, paramsId)
    val metadata = plan match {
      case s: FileSourceScanExec => s.metadata
      case s: RowDataSourceScanExec => s.metadata
      case _ => Map.empty[String, String]
    }
    new SparkPlanInfo(plan.nodeName, simpleString,
      children.map(getSparkPlanInfo(_, paramLiterals, paramsId)), metadata, metrics)
  }

  private[sql] def updatePlanInfo(planInfo: SparkPlanInfo,
      paramLiterals: Array[ParamLiteral], paramsId: Int): SparkPlanInfo = {
    if ((paramLiterals ne null) && paramLiterals.length > 0) {
      val newString = SnappySession.replaceParamLiterals(planInfo.simpleString,
        paramLiterals, paramsId)
      new SparkPlanInfo(planInfo.nodeName, newString,
        planInfo.children.map(p => updatePlanInfo(p, paramLiterals, paramsId)),
        planInfo.metadata, planInfo.metrics)
    } else planInfo
  }
}

/**
 * A wrapper plan to immediately execute the child plan without having to do
 * an explicit collect. Only use for plans returning small results.
 */
case class ExecutePlan(relation: LogicalRelation,
    child: SparkPlan, preAction: () => Unit = () => ()) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = "ExecutePlan"

  override def simpleString: String = "ExecutePlan"

  private def collectRDD(sc: SparkContext, rdd: RDD[InternalRow]): Array[InternalRow] = {
    // direct RDD collect causes NPE in new Array due to (missing?) ClassTag for some reason
    val rows = sc.runJob(rdd, (iter: Iterator[InternalRow]) => iter.toArray[InternalRow])
    Array.concat(rows: _*)
  }

  protected[sql] lazy val sideEffectResult: Array[InternalRow] = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val sc = session.sparkContext
    // Insert operations should use the configured value for retries
    // (e.g. can be disabled via the session property)
    child.collectFirst {
      case _: ColumnInsertExec | _: RowInsertExec =>
        sc.setLocalProperty(Property.MaxRetryAttemptsForWrite.name,
          Property.MaxRetryAttemptsForWrite.get(session.sessionState.conf).toString)
    }

    try {
      val key = session.currentKey
      val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      val (result, shuffleIds) = if (oldExecutionId eq null) {
        val (queryStringShortForm, queryStr, queryExecStr, planInfo) = if (key eq null) {
          val callSite = sqlContext.sparkContext.getCallSite()
          (callSite.shortForm, callSite.longForm, treeString(verbose = true),
              PartitionedPhysicalScan.getSparkPlanInfo(this))
        } else {
          val paramLiterals = key.paramLiterals
          val paramsId = key.paramsId
          (key.sqlText, key.sqlText, SnappySession.replaceParamLiterals(
            treeString(verbose = true), paramLiterals, paramsId), PartitionedPhysicalScan
              .getSparkPlanInfo(this, paramLiterals, paramsId))
        }
        CachedDataFrame.withNewExecutionId(session, child, queryStringShortForm,
          queryStr, queryExecStr, planInfo) {
          preAction()
          val rdd = child.execute()
          val shuffleIds = SnappySession.findShuffleDependencies(rdd)
          (collectRDD(sc, rdd), shuffleIds)
        }._1
      } else {
        preAction()
        val rdd = child.execute()
        val shuffleIds = SnappySession.findShuffleDependencies(rdd)
        (collectRDD(sc, rdd), shuffleIds)
      }
      if (shuffleIds.nonEmpty) {
        sc.cleaner match {
          case Some(c) => shuffleIds.foreach(c.doCleanupShuffle(_, blocking = false))
          case None =>
        }
      }

      child match {
        case _: TableExec | _: ColumnPutIntoExec =>
          // Re-cache all cached plans (including this relation itself, if it's cached)
          // that refer to this data source relation.
          session.sharedState.cacheManager.recacheByPlan(session, relation)
        case _ =>
      }

      result
    } finally {
      logDebug(s"Unlocking table in execute of ExecutePlan: ${child.treeString(verbose = false)}")
      sc.setLocalProperty(Property.MaxRetryAttemptsForWrite.name, null)
      session.clearWriteLockOnTable()
    }
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult

  override def executeTake(limit: Int): Array[InternalRow] =
    sideEffectResult.take(limit)

  override protected def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def table: String

  def region: LocalRegion

  def schema: StructType

  def numBuckets: Int

  def isPartitioned: Boolean

  def partitionColumns: Seq[String]

  def connectionType: ConnectionType.Value

  def getColocatedTable: Option[String]
}

/** Combines two SparkPlan or one SparkPlan and another RDD and acts as a LeafExecNode for the
 * higher operators.  Typical usage is like combining additional plan or rdd with
 * ColumnTableScan without breaking WholeStageCodegen.
 *
 * @param basePlan      left plan that must be code generated.
 * @param basePartKeys  left partitioner expression
 * @param otherPlan     optional. otherRDD can be used instead of this.
 * @param otherPartKeys right partitioner expression
 */
private[sql] final case class ZipPartitionScan(basePlan: CodegenSupport,
    basePartKeys: Seq[Expression],
    otherPlan: SparkPlan,
    otherPartKeys: Seq[Expression]) extends SparkPlan with CodegenSupportSnappy
    with NonRecursivePlans with SparkSupport {

  private var consumedCode: String = _
  private val consumedVars: ArrayBuffer[ExprCode] = ArrayBuffer.empty

  private val withShuffle = internals.newShuffleExchange(HashPartitioning(
    otherPartKeys, basePlan.inputRDDs().head.getNumPartitions), otherPlan)

  override def needCopyResult: Boolean = false

  override def needStopCheck: Boolean = parent.needStopCheck

  override def children: Seq[SparkPlan] = basePlan :: withShuffle :: Nil

  override def requiredChildDistribution: Seq[Distribution] = {
    internals.newHashClusteredDistribution(basePartKeys) ::
        internals.newHashClusteredDistribution(otherPartKeys) :: Nil
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    basePlan.inputRDDs ++ Some(withShuffle.execute())

  override protected def doProduce(ctx: CodegenContext): String = {
    val child1Produce = basePlan.produce(ctx, this)
    val input = internals.addClassField(ctx, "scala.collection.Iterator", "input",
      v => s"$v = inputs[1];")

    val row = ctx.freshName("row")
    val columnsInputEval = otherPlan.output.zipWithIndex.map { case (ref, ordinal) =>
      val baseIndex = ordinal
      val ev = consumedVars(ordinal)
      val evIsNull = internals.exprCodeIsNull(ev)
      val evValue = internals.exprCodeValue(ev)
      val dataType = ref.dataType
      val javaType = internals.javaType(dataType, ctx)
      val value = internals.getValue(row, dataType, baseIndex.toString, ctx)
      if (ref.nullable) {
        s"""
            boolean $evIsNull = $row.isNullAt($ordinal);
            $javaType $evValue = $evIsNull ? ${internals.defaultValue(dataType, ctx)} : ($value);
            """
      } else {
        s"""$javaType $evValue = $value;"""
      }
    }.mkString("\n")

    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  // numOutputRows.add(1);
       |  $columnsInputEval
       |  $consumedCode
       |  if (shouldStop()) return;
       |}
       |$child1Produce
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val consumeInput = evaluateVariables(input)
    consumedCode = consume(ctx, input)
    consumedVars.clear()
    input.map(_.copy()).foreach(consumedVars += _)
    consumeInput + "\n" + consumedCode
  }

  override def output: Seq[Attribute] = basePlan.output
}

/**
 * Extends Spark's ScalarSubquery to avoid emitting a constant in generated
 * code rather pass as a reference object using [[TokenLiteral]] to enable
 * generated code re-use.
 */
final class TokenizedScalarSubquery(_plan: SubqueryExec, _exprId: ExprId)
    extends ScalarSubquery(_plan, _exprId) {

  override def copy(plan: SubqueryExec = plan, exprId: ExprId = exprId): ScalarSubquery =
    new TokenizedScalarSubquery(plan, exprId)

  override def withNewPlan(query: SubqueryExec): ScalarSubquery = copy(plan = query)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val result = CatalystTypeConverters.convertToCatalyst(super.eval(null))
    new TokenLiteral(result, dataType).doGenCode(ctx, ev)
  }
}

class StratumInternalRow(val weight: Long) extends InternalRow {

  var actualRow: InternalRow = _

  def numFields: Int = throw new UnsupportedOperationException("not implemented")

  def getUTF8String(ordinal: Int): UTF8String = throw new UnsupportedOperationException("not " +
      "implemented")

  def copy(): InternalRow = throw new UnsupportedOperationException("not implemented")

  override def anyNull: Boolean = throw new UnsupportedOperationException("not implemented")

  def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException("not implemented")

  def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException("not implemented")

  def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException("not implemented")

  def getShort(ordinal: Int): Short = throw new UnsupportedOperationException("not implemented")

  def getInt(ordinal: Int): Int = throw new UnsupportedOperationException("not implemented")

  def getLong(ordinal: Int): Long = throw new UnsupportedOperationException("not implemented")

  def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException("not implemented")

  def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException("not implemented")

  def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException("not implemented")


  def getBinary(ordinal: Int): Array[Byte] =
    throw new UnsupportedOperationException("not implemented")

  def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException("not implemented")

  def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException("not implemented")

  def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException("not implemented")

  def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException("not implemented")

  def get(ordinal: Int, dataType: DataType): Object =
    throw new UnsupportedOperationException("not implemented")

  override def setNullAt(i: Int): Unit =
    throw new UnsupportedOperationException("not implemented")

  override def update(i: Int, value: Any): Unit =
    throw new UnsupportedOperationException("not implemented")
}

trait BatchConsumer extends CodegenSupportSnappy {

  /**
   * Returns true if the given plan returning batches of data can be consumed
   * by this plan.
   */
  def canConsume(plan: SparkPlan): Boolean

  /**
   * Generate Java source code to do any processing before a batch is consumed
   * by a [[PartitionedPhysicalScan]] that does batch processing (e.g. per-batch
   * optimizations, initializations etc).
   * <p>
   * Implementations should use this for additional optimizations that can be
   * done at batch level when a batched scan is being done. They should not
   * depend on this being invoked since many scans will not be batched.
   */
  def batchConsume(ctx: CodegenContext, plan: SparkPlan,
      input: Seq[ExprCode]): String

  /**
   * Generate Java source code to do any processing before return after
   * current row processing i.e. when shouldStop() returns true.
   */
  def beforeStop(ctx: CodegenContext, plan: SparkPlan,
      input: Seq[ExprCode]): String = ""
}

/**
 * Extended information for ExprCode variable to also hold the variable having
 * dictionary reference and its index when dictionary encoding is being used.
 */
case class DictionaryCode(dictionary: ExprCode, bufferVar: String,
    dictionaryIndex: ExprCode) extends SparkSupport {

  private def evaluate(ev: ExprCode): String = {
    val code = ev.code.toString
    if (code.isEmpty) ""
    else {
      internals.resetCode(ev)
      code
    }
  }

  def evaluateDictionaryCode(): String = evaluate(dictionary)

  def evaluateIndexCode(): String = evaluate(dictionaryIndex)
}

/**
 * Intermediate trait to accommodate differences in statistics method in Spark versions.
 */
trait LogicalPlanLike {

  def statistics: Statistics

  def computeStats(): Statistics
}
