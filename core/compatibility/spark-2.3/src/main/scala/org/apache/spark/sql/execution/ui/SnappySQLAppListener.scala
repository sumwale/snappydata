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

package org.apache.spark.sql.execution.ui

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.{CachedDataFrame, SparkListenerSQLPlanExecutionEnd, SparkListenerSQLPlanExecutionStart}
import org.apache.spark.status.ElementTrackingStore

/**
 * SnappyData's SQL Listener. This extends Spark's SQL listener to handle
 * combining the two part execution with CachedDataFrame where first execution
 * does the caching ("prepare" phase) along with the actual execution while subsequent
 * executions only do the latter. This listener also shortens the SQL string
 * to display properly in the UI (CachedDataFrame already takes care of posting
 * the SQL string rather than method name unlike Spark).
 */
class SnappySQLAppListener(conf: SparkConf, kvStore: ElementTrackingStore,
    originalListener: Option[SQLAppStatusListener] = None)
    extends SQLAppStatusListener(conf, kvStore, live = true) {

  private[this] val baseFields = classOf[SQLAppStatusListener].getDeclaredFields.toIndexedSeq

  private[this] def internalBaseMapField[K, V](fieldName: String): ConcurrentMap[K, V] = {
    val f = baseFields.find(_.getName.contains(fieldName)).get
    f.setAccessible(true)
    val m = f.get(this).asInstanceOf[ConcurrentMap[K, V]]
    originalListener match {
      case Some(l) => m.putAll(f.get(l).asInstanceOf[ConcurrentMap[K, V]])
      case _ =>
    }
    m
  }

  private[this] val baseLiveExecutions: ConcurrentMap[Long, LiveExecutionData] =
    internalBaseMapField[Long, LiveExecutionData]("liveExecutions")
  private[this] val baseStageMetrics: ConcurrentMap[Int, LiveStageMetrics] =
    internalBaseMapField[Int, LiveStageMetrics]("stageMetrics")

  /**
   * Executions whose planning stage has ended but execution hasn't started.
   */
  private[this] val plannedExecutions =
    new ConcurrentHashMap[Long, (LiveExecutionData, Seq[(Int, LiveStageMetrics)])]()
  private[this] val plannedExecutionsMaxSize: Int = conf.get(StaticSQLConf.UI_RETAINED_EXECUTIONS)

  /**
   * Snappy's execution happens in two phases. First phase the plan is executed
   * to create a rdd which is then used to create a CachedDataFrame.
   * In second phase, the CachedDataFrame is then used for further actions.
   * For accumulating the metrics for first phase, SparkListenerSQLPlanExecutionStart
   * is fired. This adds the query to the active executions like normal executions but
   * notes it for future full execution if required. This ensures that query is shown
   * in the UI and  new jobs that are run while the plan is being executed are tracked
   * against this executionID. In the second phase, when the query is
   * actually executed, SparkListenerSQLExecutionStart updates the execution
   * data in the active executions from existing one. SparkListenerSQLExecutionEnd is
   * then sent with the accumulated time of both the phases.
   */
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerSQLPlanExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time) =>
      super.onOtherEvent(SparkListenerSQLExecutionStart(executionId, description, details,
        physicalPlanDescription, sparkPlanInfo, time))

    case SparkListenerSQLExecutionStart(executionId, desc, details,
    physicalPlanDescription, sparkPlanInfo, time) =>

      val description =
        if (desc eq details) {
          // description and details strings being reference equals so trim off former here
          CachedDataFrame.queryStringShortForm(details)
        } else desc

      // check if execution was previously started by SparkListenerSQLPlanExecutionStart
      // and restore the jobs/metrics data during planning phase
      plannedExecutions.remove(executionId) match {
        case null =>
          if (desc ne description) {
            super.onOtherEvent(SparkListenerSQLExecutionStart(executionId, description, details,
              physicalPlanDescription, sparkPlanInfo, time))
          } else super.onOtherEvent(event)
        case (executionData, stageMetrics) =>
          executionData.description = description
          executionData.details = details
          executionData.physicalPlanDescription = physicalPlanDescription
          executionData.submissionTime = time
          executionData.completionTime = None // started again
          executionData.metricsValues = null // clear the aggregated metricValues
          executionData.endEvents -= 1 // remove SparkListenerSQLPlanExecutionEnd's endEvent
          // write immediately into KVStore (at least completionTime has changed)
          executionData.write(kvStore, System.nanoTime())
          baseLiveExecutions.put(executionId, executionData)
          // push stageMetrics into the global tracking map
          if (stageMetrics.nonEmpty) stageMetrics.foreach(p => baseStageMetrics.put(p._1, p._2))
      }

    case SparkListenerSQLPlanExecutionEnd(executionId, time) =>
      // SparkListenerSQLExecutionStart/End may never be fired for the query (e.g. for df.count)
      // so cleanup the live data but this will be restored on next SparkListenerSQLExecutionStart
      baseLiveExecutions.get(executionId) match {
        case null =>
        case exec =>
          // push any stageMetrics for execution into driverAccumUpdates
          val stageMetrics = if (exec.stages.isEmpty) Nil else exec.stages.toSeq.collect {
            case id if baseStageMetrics.containsKey(id) => id -> baseStageMetrics.get(id)
          }
          super.onOtherEvent(SparkListenerSQLExecutionEnd(executionId, time))
          plannedExecutions.put(executionId, exec -> stageMetrics)
          if (plannedExecutions.size() > plannedExecutionsMaxSize) {
            val dummy = new java.util.Date(time + 1000000L)
            val removeId = plannedExecutions.values().asScala.minBy(_._1.completionTime match {
              case None => dummy
              case Some(d) => d
            })._1.executionId
            plannedExecutions.remove(removeId)
          }
      }

    case _ => super.onOtherEvent(event)
  }
}
