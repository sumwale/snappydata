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

import org.apache.spark.sql.execution.OptimizeMetadataOnlyQuerySuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyOptimizeMetadataOnlyQuerySuite extends OptimizeMetadataOnlyQuerySuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def overridden: Seq[String] = Seq(
    // takes long to run with SD's hive metastore so using an abridged version below
    "SPARK-21884 Fix StackOverflowError on MetadataOnlyQuery"
  )

  test("SPARK-21884 Fix StackOverflowError on MetadataOnlyQuery (abridged)") {
    withTable("t_200") {
      sql("CREATE TABLE t_200 (a INT, p INT) USING PARQUET PARTITIONED BY (p)")
      (1 to 200).foreach(p => sql(s"ALTER TABLE t_200 ADD PARTITION (p=$p)"))
      sql("SELECT COUNT(DISTINCT p) FROM t_200").collect()
    }
  }
}
