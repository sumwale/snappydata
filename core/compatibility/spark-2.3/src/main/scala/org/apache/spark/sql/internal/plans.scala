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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.SparkSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan, ResolvedHint}
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.row.{RowFormatScanRDD, RowTableScan}
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * An extension to [[ResolvedHint]] to encapsulate any kind of hint rather
 * than just broadcast.
 */
class ResolvedPlanWithHints23(child: LogicalPlan, hintInfo: HintInfo,
    val allHints: Map[String, String]) extends ResolvedHint(child, hintInfo) {

  override def productArity: Int = 3

  override def productElement(n: Int): Any = n match {
    case 0 => child
    case 1 => hints
    case 2 => allHints
  }

  override def simpleString: String =
    s"ResolvedPlanWithHints[hints = $allHints; child = ${child.simpleString}]"
}

final class ColumnTableScan23(output: Seq[Attribute], dataRDD: RDD[Any],
    otherRDDs: Seq[RDD[InternalRow]], numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    baseRelation: PartitionedDataSourceScan, relationSchema: StructType,
    allFilters: Seq[Expression], schemaAttributes: Seq[AttributeReference],
    caseSensitive: Boolean, isSampleReservoirAsRegion: Boolean)
    extends ColumnTableScan(output, dataRDD, otherRDDs, numBuckets, partitionColumns,
      partitionColumnAliases, baseRelation, relationSchema, allFilters, schemaAttributes,
      caseSensitive, isSampleReservoirAsRegion) {

  override protected def doCanonicalize(): SparkPlan = if (isCanonicalizedPlan) this else {
    var id = -1
    val newOutput = output.map { ar =>
      id += 1
      ar.withExprId(ExprId(id))
    }
    new ColumnTableScan23(newOutput, dataRDD = SparkSupport.internals.EMPTY_RDD,
      otherRDDs = Nil, numBuckets, partitionColumns = Nil, partitionColumnAliases = Nil,
      baseRelation, relationSchema, allFilters = Nil, schemaAttributes = Nil,
      caseSensitive = false, isSampleReservoirAsRegion)
  }
}

final class RowTableScan23(output: Seq[Attribute], schema: StructType, dataRDD: RDD[Any],
    numBuckets: Int, partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]], table: String,
    baseRelation: PartitionedDataSourceScan, caseSensitive: Boolean)
    extends RowTableScan(output, schema, dataRDD, numBuckets, partitionColumns,
      partitionColumnAliases, table, baseRelation, caseSensitive) {

  override protected def doCanonicalize(): SparkPlan = if (isCanonicalizedPlan) this else {
    var id = -1
    val newOutput = output.map { ar =>
      id += 1
      ar.withExprId(ExprId(id))
    }
    val rdd = dataRDD match {
      case rowRDD: RowFormatScanRDD =>
        new RowFormatFiltersWrapperRDD(rowRDD.sparkContext, rowRDD.filters)
      case _ => SparkSupport.internals.EMPTY_RDD
    }
    new RowTableScan23(newOutput, schema, dataRDD = rdd, numBuckets, partitionColumns = Nil,
      partitionColumnAliases = Nil, table, baseRelation, caseSensitive = false)
  }
}

private final class RowFormatFiltersWrapperRDD(sc: SparkContext,
    private val filters: Array[Expression]) extends EmptyRDD[Any](sc) {

  override def hashCode(): Int = 1148313427 + filters.length

  override def equals(o: Any): Boolean = o match {
    case rdd: RowFormatFiltersWrapperRDD =>
      rdd.filters.length == filters.length && (rdd.filters.length == 0 ||
          rdd.filters.indices.forall(i => rdd.filters(i).semanticEquals(filters(i))))
  }
}
