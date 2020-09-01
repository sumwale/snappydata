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

import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.sources.ResolvedDataSourceSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyResolvedDataSourceSuite extends ResolvedDataSourceSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def excluded: Seq[String] = Seq(
    // test run includes kafka module by default
    "kafka: show deploy guide for loading the external kafka module"
  )

  test("kafka") {
    assert(DataSource(sparkSession = spark, className = "kafka").providingClass.getName ===
        "org.apache.spark.sql.kafka010.KafkaSourceProvider")
  }
}
