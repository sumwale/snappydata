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

package org.apache.spark.sql.execution.oplog.impl

import scala.collection.mutable

import io.snappydata.Constant
import io.snappydata.recovery.RecoveryService

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, JdbcExtendedUtils, TableScan}
import org.apache.spark.sql.types.StructType

class OpLogFormatRelation(
    _fullTableName: String,
    override val schema: StructType,
    isColumnTable: Boolean,
    override val sqlContext: SQLContext) extends BaseRelation with TableScan with Logging {

  private val fullTableName = JdbcExtendedUtils.toUpperCase(_fullTableName)
  private val (dbName, tableName) = {
    JdbcExtendedUtils.getTableWithDatabase(fullTableName,
      session = Some(sqlContext.sparkSession), forSpark = null)
  }

  override val needConversion: Boolean = false

  private def columnBatchTableName(): String = {
    dbName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  override def buildScan(): RDD[Row] = {
    val externalColumnTableName = columnBatchTableName()
    val tableSchemas = RecoveryService.schemaStructMap
    val bucketHostMap: mutable.Map[Int, String] = mutable.Map.empty
    val schemaLowerCase = StructType(schema.map(f => f.copy(name = f.name.toLowerCase)))
    val removePattern = "(executor_).*(_)".r

    (0 until RecoveryService.getNumBuckets(dbName, tableName)._1).foreach(i => {
      bucketHostMap.put(i,
        removePattern.replaceAllIn(RecoveryService.getExecutorHost(dbName, tableName, i).head, ""))
    })

    // use type erasure to pose RDD[InternalRow] as RDD[Row]
    // (needConversion=false ensures that Spark layer treats it as RDD[InternalRow])
    new OpLogRdd(sqlContext.sparkSession, fullTableName, dbName, tableName,
      externalColumnTableName, schemaLowerCase, isColumnTable, tableSchemas,
      bucketHostMap).asInstanceOf[RDD[Row]]
  }
}
