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

package org.apache.spark.sql

import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.GemFireVersion
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.GemFireXDVersion
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext, SparkException}

/**
 * Helper trait for easy access to [[SparkInternals]] using the "internals" method.
 */
trait SparkSupport {
  protected final def internals: SparkInternals = SparkSupport.internals
}

/**
 * Load appropriate Spark version support as per the current Spark version.
 */
object SparkSupport extends Logging {

  /**
   * The default Spark version for which core will be built and must exactly match
   * the version of the embedded SnappyData Spark since this will be used on executors.
   */
  final val DEFAULT_VERSION = "2.4.7"

  private[this] val EXTENDED_VERSION_PATTERN = "([0-9]\\.[0-9]\\.[0-9])\\.[0-9]".r

  @volatile private[this] var internalImpl: SparkInternals = _

  private val INTERNAL_PACKAGE = "org.apache.spark.sql.internal"

  lazy val isEnterpriseEdition: Boolean = {
    GemFireCacheImpl.setGFXDSystem(true)
    GemFireVersion.getInstance(classOf[GemFireXDVersion], SharedUtils.GFXD_VERSION_PROPERTIES)
    GemFireVersion.isEnterpriseEdition
  }

  private lazy val aqpOverridesClass: Option[Class[_]] = {
    if (isEnterpriseEdition) {
      try {
        Some(Utils.classForName("org.apache.spark.sql.execution.SnappyContextAQPFunctions"))
      } catch {
        case NonFatal(e) =>
          // Let the user know if it failed to load AQP classes.
          logWarning(s"Failed to load AQP classes in Enterprise edition: $e")
          None
      }
    } else None
  }

  private[sql] def newContextFunctions(session: SnappySession): SnappyContextFunctions = {
    aqpOverridesClass match {
      case None => new SnappyContextFunctions(session)
      case Some(c) => c.getConstructor(classOf[SnappySession]).newInstance(session)
          .asInstanceOf[SnappyContextFunctions]
    }
  }

  /**
   * An instance of [[SnappyContextFunctions]] with null session meaning any of the methods
   * that require a session instance will fail with an NPE.
   */
  lazy val contextFunctionsStateless: SnappyContextFunctions = newContextFunctions(session = null)

  /**
   * List all the supported Spark versions below. All implementations are required to
   * have a public constructor having current SparkContext as the one argument.
   */
  private val implementations: Map[String, String] = Map(
    "2.4.7" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.6" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.5" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.4" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.3" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.2" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.1" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.4.0" -> s"$INTERNAL_PACKAGE.Spark24Internals",
    "2.3.4" -> s"$INTERNAL_PACKAGE.Spark23Internals",
    "2.3.3" -> s"$INTERNAL_PACKAGE.Spark23Internals",
    "2.3.2" -> s"$INTERNAL_PACKAGE.Spark23Internals",
    "2.3.1" -> s"$INTERNAL_PACKAGE.Spark23Internals",
    "2.3.0" -> s"$INTERNAL_PACKAGE.Spark23Internals",
    "2.1.3" -> s"$INTERNAL_PACKAGE.Spark21Internals",
    "2.1.2" -> s"$INTERNAL_PACKAGE.Spark21Internals",
    "2.1.1" -> s"$INTERNAL_PACKAGE.Spark21Internals"
  )

  /**
   * Get the appropriate [[SparkInternals]] for current SparkContext version.
   */
  def internals: SparkInternals = {
    val impl = internalImpl
    if (impl ne null) impl
    else synchronized {
      val impl = internalImpl
      if (impl ne null) impl
      else {
        val sparkVersion = org.apache.spark.SPARK_VERSION match {
          case EXTENDED_VERSION_PATTERN(v) => v
          case v => v
        }
        val implClassName = implementations.get(sparkVersion) match {
          case Some(v) => v
          case None => throw new SparkException(s"Unsupported Spark version $sparkVersion")
        }
        val implClass: Class[_] = Utils.classForName(implClassName)
        internalImpl = implClass.getConstructor(classOf[String])
            .newInstance(sparkVersion).asInstanceOf[SparkInternals]
        internalImpl
      }
    }
  }

  def internals(context: SparkContext): SparkInternals = {
    val impl = internals
    val version = context.version match {
      case EXTENDED_VERSION_PATTERN(v) => v
      case v => v
    }
    if (impl.version != version) {
      throw new IllegalStateException(s"SparkVersion mismatch: " +
          s"runtime version = ${context.version}. " +
          s"Compile version = ${impl.version}")
    }
    impl
  }

  private[sql] def clear(): Unit = synchronized {
    val impl = internalImpl
    if (impl ne null) {
      impl.clearSQLListener()
      internalImpl = null
    }
  }
}
