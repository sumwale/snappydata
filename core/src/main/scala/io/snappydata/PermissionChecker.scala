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

package io.snappydata

import java.io.Serializable
import java.util.concurrent.ConcurrentHashMap

import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet
import com.pivotal.gemfirexd.Constants
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils

class PermissionChecker extends Serializable {

  private[this] val groupToUsersMap = new ConcurrentHashMap[String, Set[String]]()
  private[this] val allowedUsers = new ConcurrentHashSet[String]()

  def isAllowed(user: String): (Boolean, String) = {
    val id = IdUtil.getUserAuthorizationId(user)
    if (allowedUsers.contains(id)) return (true, "")
    val iter = groupToUsersMap.entrySet().iterator()
    while (iter.hasNext) {
      val e = iter.next()
      if (e.getValue.contains(id)) return (true, e.getKey)
    }
    (false, "")
  }

  def addUser(user: String): Unit = allowedUsers.add(user)

  def removeUser(toBeRemovedUser: String): Unit = allowedUsers.remove(toBeRemovedUser)

  def getAllowedUsers: Array[String] = {
    if (allowedUsers.isEmpty) Utils.EMPTY_STRING_ARRAY
    else allowedUsers.toArray(Utils.EMPTY_STRING_ARRAY)
  }

  private def refreshLdapGroup(group: String): Unit = {
    val grantees = ExternalStoreUtils.getExpandedGranteesIterator(Seq(group)).collect {
      case u if !u.startsWith(Constants.LDAP_GROUP_PREFIX) => IdUtil.getUserAuthorizationId(u)
    }.toSet
    groupToUsersMap.put(group, grantees)
  }

  def addLdapGroup(group: String, updateOnly: Boolean = false): Unit = {
    if (updateOnly && !groupToUsersMap.contains(group)) return
    refreshLdapGroup(group)
  }

  def removeLdapGroup(toBeRemovedGroup: String): Unit = groupToUsersMap.remove(toBeRemovedGroup)

  def refreshOnLdapGroupRefresh(group: String): Unit = {
    refreshLdapGroup(PermissionChecker.getNameWithLDAPPrefix(group))
  }

  def getAllowedGroups: Array[String] = {
    if (groupToUsersMap.isEmpty) Utils.EMPTY_STRING_ARRAY
    else {
      groupToUsersMap.keySet().toArray(Utils.EMPTY_STRING_ARRAY).map(
        _.substring(Constants.LDAP_GROUP_PREFIX.length))
    }
  }
}

object PermissionChecker extends Logging {

  lazy val dbOwner: String = {
    Misc.getMemStore.getDatabase.getDataDictionary.getAuthorizationDatabaseOwner.toLowerCase()
  }

  def isAllowed(key: String, currentUser: String, tableSchema: String): Boolean = {
    if (currentUser.equalsIgnoreCase(tableSchema) || currentUser.equalsIgnoreCase(dbOwner)) {
      return true
    }

    val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
    if (permissionsObj == null) return false
    permissionsObj.asInstanceOf[PermissionChecker].isAllowed(currentUser)._1
  }

  /** users argument should already be normalized with IdUtil.getUserAuthorizationId */
  def addRemoveUserForKey(key: String, isGrant: Boolean, users: Seq[String]): Unit = {
    PermissionChecker.synchronized {
      val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
      val permissions = if (permissionsObj ne null) permissionsObj.asInstanceOf[PermissionChecker]
      else new PermissionChecker
      // expand the users list. Can be a mix of normal user and ldap group
      users.foreach(u => {
        if (isGrant) {
          if (u.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.addLdapGroup(u)
          else permissions.addUser(u)
        } else {
          if (u.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.removeLdapGroup(u)
          else permissions.removeUser(u)
        }
      })
      logDebug(s"Putting permission obj = $permissions against key = $key")
      Misc.getMemStore.getMetadataCmdRgn.put(key, permissions)
    }
  }

  def getNameWithLDAPPrefix(g: String): String = {
    val id = IdUtil.getUserAuthorizationId(g)
    if (id.startsWith(Constants.LDAP_GROUP_PREFIX)) id else Constants.LDAP_GROUP_PREFIX + id
  }
}
