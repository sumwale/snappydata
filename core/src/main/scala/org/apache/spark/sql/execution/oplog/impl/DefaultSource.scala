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

import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Logging
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SnappyParserConsts}

class DefaultSource extends SchemaRelationProvider with DataSourceRegister with Logging {

  override def shortName(): String = SnappyParserConsts.OPLOG_SOURCE

  override def createRelation(sqlContext: SQLContext, options: Map[String, String],
      schema: StructType): BaseRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val fullTableName = parameters.get(SnappyExternalCatalog.DBTABLE_PROPERTY) match {
      case Some(name) => name
      case _ => throw new IllegalArgumentException(
        "table name not defined while trying to create relation")
    }
    val isColumnTable = parameters.get(SnappyExternalCatalog.IS_COLUMN_TABLE) match {
      case Some(v) => v.equalsIgnoreCase("true")
      case _ => false
    }

    logInfo(s"Creating OpLog relation for $fullTableName, column=$isColumnTable, schema: $schema")
    new OpLogFormatRelation(fullTableName, schema, isColumnTable, sqlContext)
  }
}
