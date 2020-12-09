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

package org.apache.spark.repl

import java.io._

import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetLeadNodeInfoMsg
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetLeadNodeInfoMsg.DataReqType

import org.apache.spark.{SparkConf, SparkEnv}

class SnappyExecutorClassLoader(conf: SparkConf,
    env: SparkEnv,
    classUri: String,
    parent: ClassLoader,
    userClassPathFirst: Boolean) extends ExecutorClassLoader(
  conf, env, classUri, parent, userClassPathFirst) {

  override def findClassLocally(name: String): Option[Class[_]] = {
    val pathInDirectory = name.replace('.', '/') + ".class"
    var inputStream: InputStream = null
    try {
      val fullPath = s"$classUri/$pathInDirectory"
      inputStream = pullFromLead(name, fullPath)
      val bytes = readAndTransformClass(name, inputStream)
      Some(defineClass(name, bytes, 0, bytes.length))
    } catch {
      case e: ClassNotFoundException =>
        // We did not find the class
        logInfo(s"Did not load class $name from REPL class server at $uri", e)
        None
      case e: Exception =>
        // Something bad happened while checking if the class exists
        if (!(e.getClass.getName.equals("com.gemstone.gemfire.cache.execute.FunctionException")
            && name.startsWith("org.apache.spark"))) {
          logError(s"Failed to check existence of class $name on REPL class server at $uri", e)
        }
        None
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logError("Exception while closing inputStream", e)
        }
      }
    }
  }

  private def pullFromLead(name: String, sourcePath: String) = {
    val collector = new GfxdListResultCollector
    val fetchClassByteMsg = new GetLeadNodeInfoMsg(
      collector, DataReqType.GET_CLASS_BYTES, 0L, sourcePath)
    logDebug(s"Pulling class bytes for $name class from lead member")
    fetchClassByteMsg.executeFunction()
    val result = collector.getResult.get(0)
    val fileContent = result.asInstanceOf[Array[Byte]]
    new ByteArrayInputStream(fileContent)
  }
}
