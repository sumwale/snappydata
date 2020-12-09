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

package io.snappydata.remote.interpreter

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.internal.snappy.InterpreterExecute
import com.pivotal.gemfirexd.{Attribute, Constants}
import io.snappydata.gemxd.SnappySessionPerConnection
import io.snappydata.{Constant, PermissionChecker}

import org.apache.spark.Logging
import org.apache.spark.sql.execution.InterpretCodeCommand
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SnappySession}

class SnappyInterpreterExecute(sql: String, connId: java.lang.Long)
    extends InterpreterExecute with Logging {

  override def execute(user: String, authToken: String): AnyRef = {
    SnappyInterpreterExecute.init()
    val (allowed, group) = SnappyInterpreterExecute.isAllowed(user)
    if (Misc.isSecurityEnabled && !user.equalsIgnoreCase(PermissionChecker.dbOwner)) {
      if (!allowed) {
        // throw exception
        throw StandardException.newException(SQLState.AUTH_NO_EXECUTE_PERMISSION, user,
          "scala code execution", "", "ComputeDB", "Cluster")
      }
    }
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    val lp = session.sessionState.sqlParser.parsePlan(sql).asInstanceOf[InterpretCodeCommand]
    val interpreterHelper = SnappyInterpreterExecute.getOrCreateStateHolder(connId, user,
      authToken, group)
    try {
      interpreterHelper.interpret(lp.code.split("\n"), lp.options)
    } finally {
      // noinspection ScalaDeprecation
      scala.Console.setOut(System.out)
    }
  }
}

object SnappyInterpreterExecute {

  private[this] val connToHelper = new mutable.HashMap[
      Long, (String, String, RemoteInterpreterStateHolder)]

  private[this] var permissions = new PermissionChecker

  private[this] var initialized = false

  def getLoader(prop: String): ClassLoader = synchronized {
    connToHelper.find(x => x._2._3.replOutputDirStr.equals(prop)) match {
      case Some(x) => x._2._3.intp.classLoader
      case None => null
    }
  }

  def isAllowed(user: String): (Boolean, String) = synchronized(permissions.isAllowed(user))

  /** users argument should already be normalized with IdUtil.getUserAuthorizationId */
  def handleNewPermissions(grantor: String, isGrant: Boolean, users: Seq[String]): Unit = {
    if (!Misc.isSecurityEnabled) return
    synchronized {
      val dbOwner = PermissionChecker.dbOwner
      if (!grantor.equalsIgnoreCase(dbOwner)) {
        throw StandardException.newException(
          SQLState.AUTH_NO_OBJECT_PERMISSION, grantor,
          "grant/revoke of scala code execution", "ComputeDB", "Cluster")
      }
      users.foreach(u => {
        if (isGrant) {
          if (u.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.addLdapGroup(u)
          else permissions.addUser(u)
        } else {
          if (u.startsWith(Constants.LDAP_GROUP_PREFIX)) removeAGroupAndCleanup(u)
          else removeAUserAndCleanup(u)
        }
      })
      Misc.getMemStore.getMetadataCmdRgn.put(Constant.INTP_GRANT_REVOKE_KEY, permissions)
    }
  }

  private[this] def removeAUserAndCleanup(user: String): Unit = {
    permissions.removeUser(user)
    val toBeCleanedUpEntries = connToHelper.filter(
      x => x._2._1.isEmpty && user.equalsIgnoreCase(x._2._2))
    toBeCleanedUpEntries.foreach(x => {
      connToHelper.remove(x._1)
      x._2._3.close()
    })
  }

  private[this] def removeAGroupAndCleanup(group: String): Unit = {
    permissions.removeLdapGroup(group)
    val toBeCleanedUpEntries = connToHelper.filter(
      x => group.equalsIgnoreCase(x._2._1)
    )
    toBeCleanedUpEntries.foreach(x => {
      connToHelper.remove(x._1)
      x._2._3.close()
    })
  }

  def refreshOnLdapGroupRefresh(group: String): Unit = {
    synchronized {
      permissions.refreshOnLdapGroupRefresh(group)
    }
    // TODO (Optimization) Reuse the grantees list retrieved above in below method.
    updateMetaRegion(group)
  }

  private[this] def updateMetaRegion(group: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    val allAuthKeys = r.keySet().asScala.filter(s =>
      s.startsWith(Constant.EXTERNAL_TABLE_REGION_KEY_PREFIX))
    allAuthKeys.foreach(k => {
      val p = r.get(k)
      if (p != null) {
        p.asInstanceOf[PermissionChecker].addLdapGroup(
          PermissionChecker.getNameWithLDAPPrefix(group), updateOnly = true)
      }
    })
  }

  def getOrCreateStateHolder(connId: Long, user: String, authToken: String,
      group: String): RemoteInterpreterStateHolder = synchronized {
    connToHelper.getOrElseUpdate(connId,
      (group, user, new RemoteInterpreterStateHolder(connId, user, authToken)))._3
  }

  def closeRemoteInterpreter(connId: Long): Unit = synchronized {
    connToHelper.remove(connId) match {
      case Some(r) => r._3.close()
      case None => // Ignore. No interpreter got create for this session.
    }
  }

  private def init(): Unit = synchronized {
    if (initialized) return
    val key = Constant.INTP_GRANT_REVOKE_KEY
    val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
    if (permissionsObj ne null) {
      permissions = permissionsObj.asInstanceOf[PermissionChecker]
    } else {
      permissions = new PermissionChecker
    }
    initialized = true
  }

  def getScalaCodeDF(code: String,
      session: SnappySession, options: Map[String, String]): Dataset[Row] = {
    val user = session.conf.get(Attribute.USERNAME_ATTR, default = null)
    val authToken = session.conf.get(Attribute.PASSWORD_ATTR, "")
    val (allowed, group) = SnappyInterpreterExecute.isAllowed(user)
    if (Misc.isSecurityEnabled && !user.equalsIgnoreCase(PermissionChecker.dbOwner)) {
      if (!allowed) {
        // throw exception
        throw StandardException.newException(SQLState.AUTH_NO_EXECUTE_PERMISSION, user,
          "scala code execution", "", "ComputeDB", "Cluster")
      }
    }
    val id = session.id
    val helper = SnappyInterpreterExecute.getOrCreateStateHolder(id, user, authToken, group)
    try {
      helper.interpret(code.split("\n"), options) match {
        case strings: Array[String] =>
          val structType = StructType(Seq(StructField("C0", StringType)))
          session.createDataFrame(strings.map(
            x => Row.fromSeq(Seq(x))).toList.asJava, structType)
        case v => v.asInstanceOf[Dataset[Row]]
      }
    } finally {
      // noinspection ScalaDeprecation
      scala.Console.setOut(System.out)
    }
  }
}
