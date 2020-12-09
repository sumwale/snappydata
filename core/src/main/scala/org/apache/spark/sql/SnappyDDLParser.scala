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

import java.io.File

import com.pivotal.gemfirexd.Constants
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, QueryHint}
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyParserConsts.plusOrMinus
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTableType, FunctionResource, FunctionResourceType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, LogicalRelation, RefreshTable}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.streaming.StreamPlanProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming._

abstract class SnappyDDLParser(session: SnappySession) extends SnappyBaseParser with SparkSupport {

  // reserved keywords
  final def ALL: Rule0 = rule { keyword(Consts.ALL) }
  final def AND: Rule0 = rule { keyword(Consts.AND) }
  final def AS: Rule0 = rule { keyword(Consts.AS) }
  final def CASE: Rule0 = rule { keyword(Consts.CASE) }
  final def CAST: Rule0 = rule { keyword(Consts.CAST) }
  final def CREATE: Rule0 = rule { keyword(Consts.CREATE) }
  final def CROSS: Rule0 = rule { keyword(Consts.CROSS) }
  final def DISTINCT: Rule0 = rule { keyword(Consts.DISTINCT) }
  final def ELSE: Rule0 = rule { keyword(Consts.ELSE) }
  final def END: Rule0 = rule { keyword(Consts.END) }
  final def EXCEPT: Rule0 = rule { keyword(Consts.EXCEPT) }
  final def EXISTS: Rule0 = rule { keyword(Consts.EXISTS) }
  final def FALSE: Rule0 = rule { keyword(Consts.FALSE) }
  final def FROM: Rule0 = rule { keyword(Consts.FROM) }
  final def GROUP: Rule0 = rule { keyword(Consts.GROUP) }
  final def HAVING: Rule0 = rule { keyword(Consts.HAVING) }
  final def IN: Rule0 = rule { keyword(Consts.IN) }
  final def INNER: Rule0 = rule { keyword(Consts.INNER) }
  final def INTERSECT: Rule0 = rule { keyword(Consts.INTERSECT) }
  final def INTO: Rule0 = rule { keyword(Consts.INTO) }
  final def IS: Rule0 = rule { keyword(Consts.IS) }
  final def JOIN: Rule0 = rule { keyword(Consts.JOIN) }
  final def LEFT: Rule0 = rule { keyword(Consts.LEFT) }
  final def NATURAL: Rule0 = rule { keyword(Consts.NATURAL) }
  final def NOT: Rule0 = rule { keyword(Consts.NOT) }
  final def NULL: Rule0 = rule { keyword(Consts.NULL) }
  final def ON: Rule0 = rule { keyword(Consts.ON) }
  final def OR: Rule0 = rule { keyword(Consts.OR) }
  final def ORDER: Rule0 = rule { keyword(Consts.ORDER) }
  final def OUTER: Rule0 = rule { keyword(Consts.OUTER) }
  final def RIGHT: Rule0 = rule { keyword(Consts.RIGHT) }
  final def SELECT: Rule0 = rule { keyword(Consts.SELECT) }
  final def TABLE: Rule0 = rule { keyword(Consts.TABLE) }
  final def THEN: Rule0 = rule { keyword(Consts.THEN) }
  final def TO: Rule0 = rule { keyword(Consts.TO) }
  final def TRUE: Rule0 = rule { keyword(Consts.TRUE) }
  final def UNION: Rule0 = rule { keyword(Consts.UNION) }
  final def USING: Rule0 = rule { keyword(Consts.USING) }
  final def WHEN: Rule0 = rule { keyword(Consts.WHEN) }
  final def WHERE: Rule0 = rule { keyword(Consts.WHERE) }
  final def WITH: Rule0 = rule { keyword(Consts.WITH) }

  // non-reserved keywords
  final def ADD: Rule0 = rule { keyword(Consts.ADD) }
  final def ALTER: Rule0 = rule { keyword(Consts.ALTER) }
  final def ANALYZE: Rule0 = rule { keyword(Consts.ANALYZE) }
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def ASC: Rule0 = rule { keyword(Consts.ASC) }
  final def AUTHORIZATION: Rule0 = rule { keyword(Consts.AUTHORIZATION) }
  final def BETWEEN: Rule0 = rule { keyword(Consts.BETWEEN) }
  final def BOTH: Rule0 = rule { keyword(Consts.BOTH) }
  final def BUCKETS: Rule0 = rule { keyword(Consts.BUCKETS) }
  final def BY: Rule0 = rule { keyword(Consts.BY) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CALL: Rule0 = rule{ keyword(Consts.CALL) }
  final def CASCADE: Rule0 = rule { keyword(Consts.CASCADE) }
  final def CHANGE: Rule0 = rule { keyword(Consts.CHANGE) }
  final def CHECK: Rule0 = rule { keyword(Consts.CHECK) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def CLUSTER: Rule0 = rule { keyword(Consts.CLUSTER) }
  final def CLUSTERED: Rule0 = rule { keyword(Consts.CLUSTERED) }
  final def CODEGEN: Rule0 = rule { keyword(Consts.CODEGEN) }
  final def COLUMN: Rule0 = rule { keyword(Consts.COLUMN) }
  final def COLUMNS: Rule0 = rule { keyword(Consts.COLUMNS) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def COMPUTE: Rule0 = rule { keyword(Consts.COMPUTE) }
  final def CONSTRAINT: Rule0 = rule { keyword(Consts.CONSTRAINT) }
  final def COST: Rule0 = rule { keyword(Consts.COST) }
  final def CUBE: Rule0 = rule { keyword(Consts.CUBE) }
  final def CURRENT: Rule0 = rule { keyword(Consts.CURRENT) }
  final def CURRENT_DATE: Rule0 = rule { keyword(Consts.CURRENT_DATE) }
  final def CURRENT_TIMESTAMP: Rule0 = rule { keyword(Consts.CURRENT_TIMESTAMP) }
  final def CURRENT_USER: Rule0 = rule { keyword(Consts.CURRENT_USER) }
  final def DATABASE: Rule0 = rule { keywords(Consts.DATABASE, Consts.SCHEMA) }
  final def DATABASES: Rule0 = rule { keywords(Consts.DATABASES, Consts.SCHEMAS) }
  final def DBPROPERTIES: Rule0 = rule { keyword(Consts.DBPROPERTIES) }
  final def DELETE: Rule0 = rule { keyword(Consts.DELETE) }
  final def DEPLOY: Rule0 = rule { keyword(Consts.DEPLOY) }
  final def DESC: Rule0 = rule { keyword(Consts.DESC) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DIRECTORY: Rule0 = rule { keyword(Consts.DIRECTORY) }
  final def DISABLE: Rule0 = rule { keyword(Consts.DISABLE) }
  final def DISTRIBUTE: Rule0 = rule { keyword(Consts.DISTRIBUTE) }
  final def DISKSTORE: Rule0 = rule { keyword(Consts.DISKSTORE) }
  final def DIV: Rule0 = rule { keyword(Consts.DIV) }
  final def DROP: Rule0 = rule { keyword(Consts.DROP) }
  final def DURATION: Rule0 = rule { keyword(Consts.DURATION) }
  final def ENABLE: Rule0 = rule { keyword(Consts.ENABLE) }
  final def EXEC: Rule0 = rule { keyword(Consts.EXEC) }
  final def EXECUTE: Rule0 = rule { keyword(Consts.EXECUTE) }
  final def EXPLAIN: Rule0 = rule { keyword(Consts.EXPLAIN) }
  final def EXTENDED: Rule0 = rule { keyword(Consts.EXTENDED) }
  final def EXTERNAL: Rule0 = rule { keyword(Consts.EXTERNAL) }
  final def EXTRACT: Rule0 = rule { keyword(Consts.EXTRACT) }
  final def FETCH: Rule0 = rule { keyword(Consts.FETCH) }
  final def FIRST: Rule0 = rule { keyword(Consts.FIRST) }
  final def FN: Rule0 = rule { keyword(Consts.FN) }
  final def FOLLOWING: Rule0 = rule { keyword(Consts.FOLLOWING) }
  final def FOR: Rule0 = rule { keyword(Consts.FOR) }
  final def FOREIGN: Rule0 = rule { keyword(Consts.FOREIGN) }
  final def FORMAT: Rule0 = rule { keyword(Consts.FORMAT) }
  final def FORMATTED: Rule0 = rule { keyword(Consts.FORMATTED) }
  final def FULL: Rule0 = rule { keyword(Consts.FULL) }
  final def FUNCTION: Rule0 = rule { keyword(Consts.FUNCTION) }
  final def FUNCTIONS: Rule0 = rule { keyword(Consts.FUNCTIONS) }
  final def GLOBAL: Rule0 = rule { keyword(Consts.GLOBAL) }
  final def GRANT: Rule0 = rule { keyword(Consts.GRANT) }
  final def GROUPING: Rule0 = rule { keyword(Consts.GROUPING) }
  final def HASH: Rule0 = rule { keyword(Consts.HASH) }
  final def IF: Rule0 = rule { keyword(Consts.IF) }
  final def IGNORE: Rule0 = rule { keyword(Consts.IGNORE) }
  final def INDEX: Rule0 = rule { keyword(Consts.INDEX) }
  final def INIT: Rule0 = rule { keyword(Consts.INIT) }
  final def INSERT: Rule0 = rule { keyword(Consts.INSERT) }
  final def INTERVAL: Rule0 = rule { keyword(Consts.INTERVAL) }
  final def JAR: Rule0 = rule { keyword(Consts.JAR) }
  final def JARS: Rule0 = rule { keyword(Consts.JARS) }
  final def LAST: Rule0 = rule { keyword(Consts.LAST) }
  final def LATERAL: Rule0 = rule { keyword(Consts.LATERAL) }
  final def LAZY: Rule0 = rule { keyword(Consts.LAZY) }
  final def LDAPGROUP: Rule0 = rule { keyword(Consts.LDAPGROUP) }
  final def LEADING: Rule0 = rule { keyword(Consts.LEADING) }
  final def LEVEL: Rule0 = rule { keyword(Consts.LEVEL) }
  final def LIKE: Rule0 = rule { keyword(Consts.LIKE) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def LIST: Rule0 = rule { keyword(Consts.LIST) }
  final def LOAD: Rule0 = rule { keyword(Consts.LOAD) }
  final def LOCAL: Rule0 = rule { keyword(Consts.LOCAL) }
  final def LOCATION: Rule0 = rule { keyword(Consts.LOCATION) }
  final def MEMBERS: Rule0 = rule { keyword(Consts.MEMBERS) }
  final def MINUS: Rule0 = rule { keyword(Consts.SETMINUS) }
  final def MSCK: Rule0 = rule { keyword(Consts.MSCK) }
  final def NULLS: Rule0 = rule { keyword(Consts.NULLS) }
  final def OF: Rule0 = rule { keyword(Consts.OF) }
  final def ONLY: Rule0 = rule { keyword(Consts.ONLY) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def OUT: Rule0 = rule { keyword(Consts.OUT) }
  final def OVER: Rule0 = rule { keyword(Consts.OVER) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PACKAGE: Rule0 = rule { keyword(Consts.PACKAGE) }
  final def PACKAGES: Rule0 = rule { keyword(Consts.PACKAGES) }
  final def PARTITION: Rule0 = rule { keyword(Consts.PARTITION) }
  final def PARTITIONED: Rule0 = rule { keyword(Consts.PARTITIONED) }
  final def PATH: Rule0 = rule { keyword(Consts.PATH) }
  final def PERCENT: Rule0 = rule { keyword(Consts.PERCENT) }
  final def PIVOT: Rule0 = rule { keyword(Consts.PIVOT) }
  final def POLICY: Rule0 = rule { keyword(Consts.POLICY) }
  final def POSITION: Rule0 = rule { keyword(Consts.POSITION) }
  final def PRECEDING: Rule0 = rule { keyword(Consts.PRECEDING) }
  final def PRIMARY: Rule0 = rule { keyword(Consts.PRIMARY) }
  final def PRIVILEGE: Rule0 = rule { keyword(Consts.PRIVILEGE) }
  final def PURGE: Rule0 = rule { keyword(Consts.PURGE) }
  final def PUT: Rule0 = rule { keyword(Consts.PUT) }
  final def RANGE: Rule0 = rule { keyword(Consts.RANGE) }
  final def REFRESH: Rule0 = rule { keyword(Consts.REFRESH) }
  final def REGEXP: Rule0 = rule { keyword(Consts.REGEXP) }
  final def RENAME: Rule0 = rule { keyword(Consts.RENAME) }
  final def REPLACE: Rule0 = rule { keyword(Consts.REPLACE) }
  final def REPOS: Rule0 = rule { keyword(Consts.REPOS) }
  final def RESET: Rule0 = rule { keyword(Consts.RESET) }
  final def RESTRICT: Rule0 = rule { keyword(Consts.RESTRICT) }
  final def RETURNS: Rule0 = rule { keyword(Consts.RETURNS) }
  final def REVOKE: Rule0 = rule { keyword(Consts.REVOKE) }
  final def RLIKE: Rule0 = rule { keyword(Consts.RLIKE) }
  final def ROLLUP: Rule0 = rule { keyword(Consts.ROLLUP) }
  final def ROW: Rule0 = rule { keyword(Consts.ROW) }
  final def ROWS: Rule0 = rule { keyword(Consts.ROWS) }
  final def SCALA: Rule0 = rule { keyword(Consts.SCALA) }
  final def SECURITY: Rule0 = rule { keyword(Consts.SECURITY) }
  final def SEMI: Rule0 = rule { keyword(Consts.SEMI) }
  final def SERDE: Rule0 = rule { keyword(Consts.SERDE) }
  final def SERDEPROPERTIES: Rule0 = rule { keyword(Consts.SERDEPROPERTIES) }
  final def SET: Rule0 = rule { keyword(Consts.SET) }
  final def SETS: Rule0 = rule { keyword(Consts.SETS) }
  final def SHOW: Rule0 = rule { keyword(Consts.SHOW) }
  final def SKEWED: Rule0 = rule { keyword(Consts.SKEWED) }
  final def SLIDE: Rule0 = rule { keyword(Consts.SLIDE) }
  final def SORT: Rule0 = rule { keyword(Consts.SORT) }
  final def SORTED: Rule0 = rule { keyword(Consts.SORTED) }
  final def START: Rule0 = rule { keyword(Consts.START) }
  final def STATISTICS: Rule0 = rule { keyword(Consts.STATISTICS) }
  final def STOP: Rule0 = rule { keyword(Consts.STOP) }
  final def STORED: Rule0 = rule { keyword(Consts.STORED) }
  final def STREAM: Rule0 = rule { keyword(Consts.STREAM) }
  final def STREAMING: Rule0 = rule { keyword(Consts.STREAMING) }
  final def TABLES: Rule0 = rule { keyword(Consts.TABLES) }
  final def TABLESAMPLE: Rule0 = rule { keyword(Consts.TABLESAMPLE) }
  final def TBLPROPERTIES: Rule0 = rule { keyword(Consts.TBLPROPERTIES) }
  final def TEMPORARY: Rule0 = rule { keywords(Consts.TEMPORARY, Consts.TEMP) }
  final def TRAILING: Rule0 = rule { keyword(Consts.TRAILING) }
  final def TRIGGER: Rule0 = rule { keyword(Consts.TRIGGER) }
  final def TRIM: Rule0 = rule { keyword(Consts.TRIM) }
  final def TRUNCATE: Rule0 = rule { keyword(Consts.TRUNCATE) }
  final def UNBOUNDED: Rule0 = rule { keyword(Consts.UNBOUNDED) }
  final def UNCACHE: Rule0 = rule { keyword(Consts.UNCACHE) }
  final def UNDEPLOY: Rule0 = rule { keyword(Consts.UNDEPLOY) }
  final def UNIQUE: Rule0 = rule { keyword(Consts.UNIQUE) }
  final def UNSET: Rule0 = rule { keyword(Consts.UNSET) }
  final def UPDATE: Rule0 = rule { keyword(Consts.UPDATE) }
  final def USE: Rule0 = rule { keyword(Consts.USE) }
  final def USER: Rule0 = rule { keyword(Consts.USER) }
  final def VALUES: Rule0 = rule { keyword(Consts.VALUES) }
  final def VIEW: Rule0 = rule { keyword(Consts.VIEW) }
  final def VIEWS: Rule0 = rule { keyword(Consts.VIEWS) }
  final def WINDOW: Rule0 = rule { keyword(Consts.WINDOW) }

  // interval units (non-reserved)
  final def DAY: Rule0 = rule { intervalUnit(Consts.DAY) }
  final def HOUR: Rule0 = rule { intervalUnit(Consts.HOUR) }
  final def MICROS: Rule0 = rule { intervalUnit("micro") }
  final def MICROSECOND: Rule0 = rule { intervalUnit(Consts.MICROSECOND) }
  final def MILLIS: Rule0 = rule { intervalUnit("milli") }
  final def MILLISECOND: Rule0 = rule { intervalUnit(Consts.MILLISECOND) }
  final def MINS: Rule0 = rule { intervalUnit("min") }
  final def MINUTE: Rule0 = rule { intervalUnit(Consts.MINUTE) }
  final def MONTH: Rule0 = rule { intervalUnit(Consts.MONTH) }
  final def SECS: Rule0 = rule { intervalUnit("sec") }
  final def SECOND: Rule0 = rule { intervalUnit(Consts.SECOND) }
  final def WEEK: Rule0 = rule { intervalUnit(Consts.WEEK) }
  final def YEAR: Rule0 = rule { intervalUnit(Consts.YEAR) }

  @inline
  final def sparkContext: SparkContext =
    if (session ne null) session.sparkContext else SnappyContext.globalSparkContext

  final lazy val sqlConf: SQLConf =
    if (session ne null) session.sessionState.conf else {
      val conf = new SQLConf
      val sc = sparkContext
      if (sc ne null) {
        sc.conf.getAll.foreach(p => conf.setConfString(p._1, p._2))
      }
      conf
    }

  /** spark parser used for hive DDLs that are not relevant to SnappyData's builtin sources */
  protected final lazy val sparkParser: SparkSqlParser = new SparkSqlParser(sqlConf)

  @inline
  protected final def hasHiveSupport: Boolean = {
    if (session ne null) session.enableHiveSupport
    else SnappySession.isHiveSupportEnabled(sqlConf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION))
  }

  // DDLs, SET etc

  final type ColumnDirectionMap = Seq[(String, Option[SortDirection])]
  final type TableEnd = (Option[String], Option[Map[String, String]], Option[Map[String, String]],
      Option[String], Option[Seq[String]], Option[BucketSpec], Option[String], Option[LogicalPlan])

  protected final def ifNotExists: Rule1[Boolean] = rule {
    (IF ~ NOT ~ EXISTS ~ push(true)).? ~> ((o: Any) => o != None)
  }

  protected final def ifExists: Rule1[Boolean] = rule {
    (IF ~ EXISTS ~ push(true)).? ~> ((o: Any) => o != None)
  }

  protected final def identifierWithComment: Rule1[(String, Option[String])] = rule {
    identifier ~ (COMMENT ~ stringLiteral).? ~>
        ((id: String, cm: Any) => id -> cm.asInstanceOf[Option[String]])
  }

  protected final def locationSpec: Rule1[String] = rule {
    LOCATION ~ stringLiteral
  }

  protected def createHiveTable: Rule1[LogicalPlan] = rule {
    test(hasHiveSupport) ~ capture(CREATE ~ (TEMPORARY | EXTERNAL).? ~
        TABLE ~ ifNotExists ~ tableIdentifier ~ tableSchema.?) ~
        capture(USING ~ ignoreCase("hive") ~ ws | COMMENT | PARTITIONED ~ BY | CLUSTERED ~ BY |
            SKEWED ~ BY | ROW ~ FORMAT | STORED | LOCATION | TBLPROPERTIES) ~ capture(ANY.*) ~>
        ((_: Boolean, _: TableIdentifier, _: Any, head: String, k: String, tail: String) =>
          if (Utils.toLowerCase(k).startsWith("using")) sparkParser.parsePlan(head + tail)
          else sparkParser.parsePlan(head + k + tail))
  }

  protected def createTable: Rule1[LogicalPlan] = rule {
    CREATE ~ (EXTERNAL ~ push(true)).? ~ TABLE ~ ifNotExists ~
        tableIdentifier ~ tableEnd ~> { (external: Any, allowExisting: Boolean,
        tableIdent: TableIdentifier, schemaStr: StringBuilder, remaining: TableEnd) =>

      val options = remaining._2 match {
        case None => Map.empty[String, String]
        case Some(m) => m
      }
      val props = remaining._3 match {
        case None => Map.empty[String, String]
        case Some(m) => m
      }
      val partitionCols = remaining._5 match {
        case None => Nil
        case Some(s) => s
      }
      val provider = remaining._1 match {
        case None =>
          // use hive source as default if appropriate is set else use 'row'
          if (hasHiveSupport && sparkCompatible) {
            DDLUtils.HIVE_PROVIDER
          } else {
            sqlConf.getConfString(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, Consts.DEFAULT_SOURCE)
          }
        case Some(p) => p
      }
      // check if hive provider is being used
      if (provider.equalsIgnoreCase(DDLUtils.HIVE_PROVIDER) && hasHiveSupport) {
        sparkParser.parsePlan(input.sliceString(0, input.length))
      } else {
        val schemaString = schemaStr.toString().trim
        // check if a relation supporting free-form schema has been used that supports
        // syntax beyond Spark support
        val (userSpecifiedSchema, schemaDDL) = if (schemaString.length > 0) {
          if (ExternalStoreUtils.isExternalSchemaRelationProvider(provider, sqlConf)) {
            None -> Some(schemaString)
          } else synchronized {
            // parse the schema string expecting Spark SQL format
            val colParser = newInstance()
            colParser.parseSQLOnly(schemaString, colParser.tableSchemaOpt.run())
                .map(StructType(_)) -> None
          }
        } else None -> None

        // When IF NOT EXISTS clause appears in the query,
        // the save mode will be ignore.
        val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTableUsingCommand(tableIdent, None, userSpecifiedSchema, schemaDDL,
          provider, mode, options, props, partitionCols, remaining._6, remaining._8,
          external != None, comment = remaining._4, location = remaining._7)
      }
    }
  }

  protected def createTableLike: Rule1[LogicalPlan] = rule {
    CREATE ~ TABLE ~ ifNotExists ~ tableIdentifier ~ LIKE ~ tableIdentifier ~
        locationSpec.? ~> ((allowExisting: Boolean, targetIdent: TableIdentifier,
        sourceIdent: TableIdentifier, location: Any) => internals.newCreateTableLikeCommand(
      targetIdent, sourceIdent, location.asInstanceOf[Option[String]], allowExisting))
  }

  protected final def booleanLiteral: Rule1[Boolean] = rule {
    TRUE ~> (() => true) | FALSE ~> (() => false)
  }

  protected final def numericLiteral: Rule1[String] = rule {
    capture(plusOrMinus.? ~ Consts.numeric. + ~ (Consts.exponent ~
        plusOrMinus.? ~ CharPredicate.Digit. +).? ~ Consts.numericSuffix.? ~
        Consts.numericSuffix.?) ~ delimiter ~> ((s: String) => s)
  }

  protected final def groupOrUser: Rule1[String] = rule {
    LDAPGROUP ~ ':' ~ ws ~ identifier ~> ((group: String) =>
      Constants.LDAP_GROUP_PREFIX + IdUtil.getUserAuthorizationId(group)) |
    identifier ~> ((user: String) => IdUtil.getUserAuthorizationId(user))
  }

  protected final def policyFor: Rule1[String] = rule {
    (FOR ~ capture(ALL | SELECT | UPDATE | INSERT | DELETE)).? ~> ((forOpt: Any) =>
      forOpt match {
        case Some(v) => v.asInstanceOf[String].trim
        case None => SnappyParserConsts.SELECT.lower
      })
  }

  protected final def policyTo: Rule1[Seq[String]] = rule {
    (TO ~
        ((CURRENT_USER ~ push(SnappyParserConsts.CURRENT_USER.lower) | groupOrUser) + commaSep) ~>
        ((policyTo: Any) => policyTo.asInstanceOf[Seq[String]])
    ).? ~> ((toOpt: Any) => toOpt match {
      case Some(x) => x.asInstanceOf[Seq[String]]
      case _ => SnappyParserConsts.CURRENT_USER.lower :: Nil
    })
  }

  protected def createPolicy: Rule1[LogicalPlan] = rule {
    (CREATE ~ POLICY) ~ tableIdentifier ~ ON ~ tableIdentifier ~ policyFor ~
        policyTo ~ USING ~ capture(expression) ~> { (policyName: TableIdentifier,
        tableName: TableIdentifier, policyFor: String,
        applyTo: Seq[String], filterExp: Expression, filterStr: String) =>
      val applyToAll = applyTo.exists(_.equalsIgnoreCase(
        SnappyParserConsts.CURRENT_USER.lower))
      val expandedApplyTo = if (applyToAll) Nil
      else ExternalStoreUtils.getExpandedGranteesIterator(applyTo).toSeq
      /*
      val targetRelation = snappySession.sessionState.catalog.lookupRelation(tableIdent)
      val isTargetExternalRelation =  targetRelation.find(x => x match {
        case _: ExternalRelation => true
        case _ => false
      }).isDefined
      */
      // use normalized value for string comparison in filters
      val currentUser = IdUtil.getUserAuthorizationId(sqlConf.getConfString(
        com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, Constant.DEFAULT_DATABASE))
      val filter = PolicyProperties.createFilterPlan(filterExp, tableName,
        currentUser, expandedApplyTo)

      CreatePolicyCommand(policyName, tableName, policyFor, applyTo, expandedApplyTo,
        currentUser, filterStr, filter)
    }
  }

  protected def dropPolicy: Rule1[LogicalPlan] = rule {
    DROP ~ POLICY ~ ifExists ~ tableIdentifier ~> DropPolicyCommand
  }

  protected final def beforeDDLEnd: Rule0 = rule {
    noneOf("uUoOaA-;/")
  }

  protected final def identifierList: Rule1[Seq[String]] = rule {
    O_PAREN ~ (identifierExt1 + commaSep) ~ C_PAREN
  }

  protected final def bucketSpec: Rule1[BucketSpec] = rule {
    CLUSTERED ~ BY ~ identifierList ~ (SORTED ~ BY ~ colsWithDirection).? ~
        INTO ~ integral ~ BUCKETS ~> ((cols: Seq[String], sort: Any, buckets: String) =>
      sort match {
        case None => BucketSpec(buckets.toInt, cols, Nil)
        case Some(m) =>
          val sortColumns = m.asInstanceOf[ColumnDirectionMap].map {
            case (_, Some(Descending)) => throw Utils.analysisException(
              s"Column ordering for buckets must be ASC but was DESC")
            case (c, _) => c
          }
          BucketSpec(buckets.toInt, cols, sortColumns)
      })
  }

  private def checkDuplicate(opts: Array[Option[Any]], index: Int, v: Some[Any],
      clause: String): Unit = {
    if (opts(index).isDefined) {
      throw new ParseException(s"Found duplicate clauses: $clause")
    }
    opts(index) = v
  }

  protected final def ddlEnd: Rule1[TableEnd] = rule {
    ws ~ (USING ~ qualifiedName).? ~ (OPTIONS ~ options |
        TBLPROPERTIES ~ options ~> ((m: Map[String, String]) => Some(m)) |
        COMMENT ~ stringLiteral ~> ((s: String) => Some(s)) |
        PARTITIONED ~ BY ~ identifierList | bucketSpec | locationSpec).* ~
        (AS ~ query).? ~ ws ~ &((';' ~ ws).* ~ EOI) ~>
        ((provider: Any, optionals: Any, asQuery: Any) => {
          // options, comment, partitions, buckets, location
          val tableOpts = Array[Option[Any]](None, None, None, None, None, None)
          optionals.asInstanceOf[Seq[Any]].foreach {
            case opts: Map[_, _] => checkDuplicate(tableOpts, 0, Some(opts), "OPTIONS")
            case props: Some[_] if props.get.isInstanceOf[Map[_, _]] =>
              checkDuplicate(tableOpts, 1, props, "TBLPROPERTIES")
            case comment: Some[_] => checkDuplicate(tableOpts, 2, comment, "COMMENT")
            case parts: Seq[_] => checkDuplicate(tableOpts, 3,
              Some(parts.asInstanceOf[Seq[String]]), "PARTITIONED BY")
            case buckets: BucketSpec => checkDuplicate(tableOpts, 4, Some(buckets), "CLUSTERED BY")
            case location: String => checkDuplicate(tableOpts, 5, Some(location), "LOCATION")
            case v => throw new ParseException(s"Unknown table clause: $v")
          }
          (provider, tableOpts(0), tableOpts(1), tableOpts(2), tableOpts(3), tableOpts(4),
              tableOpts(5), asQuery).asInstanceOf[TableEnd]
        })
  }

  protected final def tableEnd1: Rule[StringBuilder :: HNil,
      StringBuilder :: TableEnd :: HNil] = rule {
    ddlEnd.asInstanceOf[Rule[StringBuilder :: HNil,
        StringBuilder :: TableEnd :: HNil]] |
    // no free form pass through to store/spark-hive layer if USING has been provided
    // to detect genuine syntax errors correctly rather than store throwing
    // some irrelevant error
    (!(ws ~ (USING ~ qualifiedName | (OPTIONS | TBLPROPERTIES) ~ options)) ~ capture(ANY ~
        beforeDDLEnd.*) ~> ((s: StringBuilder, n: String) => s.append(n))) ~ tableEnd1
  }

  protected final def tableEnd: Rule2[StringBuilder, TableEnd] = rule {
    (capture(beforeDDLEnd.*) ~> ((s: String) =>
      new StringBuilder().append(s))) ~ tableEnd1
  }

  protected def interpretCode: Rule1[LogicalPlan] = rule {
    EXEC ~ SCALA ~ (OPTIONS ~ options).? ~ capture(ANY.*) ~> ((opts: Any, code: String) =>
      opts.asInstanceOf[Option[Map[String, String]]] match {
        case None => InterpretCodeCommand(code, session)
        case Some(m) => InterpretCodeCommand(code, session, m)
      })
  }

  protected def grantRevokeInterpreter: Rule1[LogicalPlan] = rule {
    GRANT ~ PRIVILEGE ~ EXEC ~ SCALA ~ TO ~ (groupOrUser + commaSep) ~> ((users: Seq[String]) =>
      GrantRevokeInterpreterCommand(isGrant = true, users)) |
    REVOKE ~ PRIVILEGE ~ EXEC ~ SCALA ~ FROM ~ (groupOrUser + commaSep) ~> ((users: Seq[String]) =>
      GrantRevokeInterpreterCommand(isGrant = false, users))
  }

  protected def createIndex: Rule1[LogicalPlan] = rule {
    (CREATE ~ (GLOBAL ~ HASH ~ push(false) | UNIQUE ~ push(true)).? ~ INDEX) ~
        tableIdentifier ~ ON ~ tableIdentifier ~
        colsWithDirection ~ (OPTIONS ~ options).? ~> {
      (indexType: Any, indexName: TableIdentifier, tableName: TableIdentifier,
          cols: ColumnDirectionMap, opts: Any) =>
        val parameters = opts.asInstanceOf[Option[Map[String, String]]]
            .getOrElse(Map.empty[String, String])
        val options = indexType.asInstanceOf[Option[Boolean]] match {
          case Some(false) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "global hash")
          case Some(true) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "unique")
          case None => parameters
        }
        CreateIndexCommand(indexName, tableName, cols, options)
    }
  }

  protected final def globalOrTemporary: Rule1[Boolean] = rule {
    (GLOBAL ~ push(true)).? ~ TEMPORARY ~> ((g: Any) => g != None)
  }

  protected def createView: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ (globalOrTemporary.? ~ VIEW |
        globalOrTemporary ~ TABLE) ~ ifNotExists ~ tableIdentifier ~
        (O_PAREN ~ (identifierWithComment + commaSep) ~ C_PAREN).? ~
        (COMMENT ~ stringLiteral).? ~ ((OPTIONS | TBLPROPERTIES) ~ options).? ~
        AS ~ capture(query) ~> { (replace: Any, gt: Any,
        allowExisting: Boolean, table: TableIdentifier, cols: Any, comment: Any,
        opts: Any, plan: LogicalPlan, queryStr: String) =>

      val viewType = gt match {
        case Some(true) | true => GlobalTempView
        case Some(false) | false => LocalTempView
        case _ => PersistedView
      }
      val userCols = cols.asInstanceOf[Option[Seq[(String, Option[String])]]] match {
        case Some(seq) => seq
        case None => Nil
      }
      val viewOpts = opts.asInstanceOf[Option[Map[String, String]]] match {
        case Some(m) => m
        case None => Map.empty[String, String]
      }
      CreateViewCommand(
        name = table,
        userSpecifiedColumns = userCols,
        comment = comment.asInstanceOf[Option[String]],
        properties = viewOpts,
        originalText = Option(queryStr),
        child = plan,
        allowExisting = allowExisting,
        replace = replace != None,
        viewType = viewType)
    }
  }

  protected def createTempViewUsing: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ globalOrTemporary ~ (VIEW ~ push(false) |
        TABLE ~ push(true)) ~ tableIdentifier ~ tableSchema.? ~ USING ~ qualifiedName ~
        (OPTIONS ~ options).? ~> ((replace: Any, global: Boolean, isTable: Boolean,
        table: TableIdentifier, schema: Any, provider: String, opts: Any) => CreateTempViewUsing(
      tableIdent = table,
      userSpecifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]].map(StructType(_)),
      // in Spark replace is always true for CREATE TEMPORARY TABLE
      replace = replace != None || (!global && isTable),
      global = global,
      provider = provider,
      options = opts.asInstanceOf[Option[Map[String, String]]].getOrElse(Map.empty)))
  }

  protected def dropIndex: Rule1[LogicalPlan] = rule {
    DROP ~ INDEX ~ ifExists ~ tableIdentifier ~> DropIndexCommand
  }

  protected def dropTable: Rule1[LogicalPlan] = rule {
    DROP ~ TABLE ~ ifExists ~ tableIdentifier ~ (PURGE ~ push(true)).? ~>
        ((exists: Boolean, table: TableIdentifier, purge: Any) =>
          DropTableOrViewCommand(table, exists, isView = false, purge = purge != None))
  }

  protected def dropView: Rule1[LogicalPlan] = rule {
    DROP ~ VIEW ~ ifExists ~ tableIdentifier ~> ((exists: Boolean, table: TableIdentifier) =>
      DropTableOrViewCommand(table, exists, isView = true, purge = false))
  }

  protected def alterView: Rule1[LogicalPlan] = rule {
    ALTER ~ VIEW ~ tableIdentifier ~ AS.? ~ capture(query) ~> ((name: TableIdentifier,
        plan: LogicalPlan, queryStr: String) => AlterViewAsCommand(name, queryStr, plan))
  }

  protected def createDatabase: Rule1[LogicalPlan] = rule {
    CREATE ~ DATABASE ~ ifNotExists ~ identifier ~ (COMMENT ~ stringLiteral).? ~
        locationSpec.? ~ (WITH ~ DBPROPERTIES ~ options).? ~ (AUTHORIZATION ~ groupOrUser).? ~> {
      (notExists: Boolean, schemaName: String, c: Any, loc: Any, opts: Any, authId: Any) =>
        var props = opts.asInstanceOf[Option[Map[String, String]]] match {
          case None => Map.empty[String, String]
          case Some(m) => m
        }
        authId.asInstanceOf[Option[String]] match {
          case Some(id) => props += Consts.AUTHORIZATION.lower -> id
          case _ =>
        }
        CreateDatabaseCommand(schemaName, notExists, loc.asInstanceOf[Option[String]],
          c.asInstanceOf[Option[String]], props)
    }
  }

  protected def dropDatabase: Rule1[LogicalPlan] = rule {
    DROP ~ DATABASE ~ ifExists ~ identifier ~ (RESTRICT ~ push(false) | CASCADE ~ push(true)).? ~>
        ((exists: Boolean, databaseName: String, cascade: Any) => DropDatabaseCommand(
          databaseName, exists, cascade.asInstanceOf[Option[Boolean]].contains(true)))
  }

  protected def truncateTable: Rule1[LogicalPlan] = rule {
    TRUNCATE ~ TABLE ~ ifExists ~ tableIdentifier ~> TruncateManagedTableCommand
  }

  protected def alterTableToggleRowLevelSecurity: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ ((ENABLE ~ push(true)) | (DISABLE ~ push(false))) ~
        ROW ~ LEVEL ~ SECURITY ~> {
      (tableName: TableIdentifier, enbableRLS: Boolean) =>
        AlterTableToggleRowLevelSecurityCommand(tableName, enbableRLS)
    }
  }

  protected final def canAlter(id: TableIdentifier, op: String,
      allBuiltins: Boolean = false): TableIdentifier = {
    val catalogTable = session.sessionState.catalog.getTempViewOrPermanentTableMetadata(id)
    if (catalogTable.tableType != CatalogTableType.VIEW) {
      val objectType = CatalogObjectType.getTableType(catalogTable)
      // many alter commands are not supported for tables backed by snappy-store and topK
      if ((allBuiltins && !(objectType == CatalogObjectType.External ||
          objectType == CatalogObjectType.Hive)) ||
          (!allBuiltins && (CatalogObjectType.isTableBackedByRegion(objectType) ||
              objectType == CatalogObjectType.TopK))) {
        throw Utils.analysisException(
          s"ALTER TABLE... $op for table $id not supported by provider ${catalogTable.provider}")
      }
    }
    id
  }

  private def toPartSpec(spec: Map[String, Option[String]]): Option[Map[String, String]] = {
    if (spec.isEmpty) None
    else Some(spec.mapValues {
      case None => null
      case Some(v) => v
    })
  }

  private final def alterTableProps: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ partitionSpec ~ SET ~ (
        SERDEPROPERTIES ~ options ~> ((id: TableIdentifier, partSpec: Map[String, Option[String]],
            opts: Map[String, String]) => AlterTableSerDePropertiesCommand(canAlter(
          id, "SET SERDEPROPERTIES"), None, Some(opts), toPartSpec(partSpec))) |
        SERDE ~ stringLiteral ~ (WITH ~ SERDEPROPERTIES ~ options).? ~>
            ((id: TableIdentifier, partSpec: Map[String, Option[String]], n: String, opts: Any) =>
              AlterTableSerDePropertiesCommand(canAlter(id, "SET SERDE", allBuiltins = true),
                Some(n), opts.asInstanceOf[Option[Map[String, String]]], toPartSpec(partSpec))) |
        locationSpec ~> ((id: TableIdentifier, partSpec: Map[String, Option[String]],
            path: String) => AlterTableSetLocationCommand(canAlter(
          id, "SET LOCATION", allBuiltins = true), toPartSpec(partSpec), path))
    )
  }

  protected def alterTableOrView: Rule1[LogicalPlan] = rule {
    ALTER ~ (TABLE ~ push(false) | VIEW ~ push(true)) ~ tableIdentifier ~ (
        RENAME ~ TO ~ tableIdentifier ~> ((view: Boolean, from: TableIdentifier,
            to: TableIdentifier) => AlterTableRenameCommand(canAlter(from, "RENAME"), to, view)) |
        SET ~ TBLPROPERTIES ~ options ~> ((view: Boolean, id: TableIdentifier,
            opts: Map[String, String]) => AlterTableSetPropertiesCommand(canAlter(
          id, "SET TBLPROPERTIES"), opts, view)) |
        UNSET ~ TBLPROPERTIES ~ (IF ~ EXISTS ~ push(true)).? ~
            O_PAREN ~ (optionKey + commaSep) ~ C_PAREN ~> ((view: Boolean,
            id: TableIdentifier, exists: Any, keys: Seq[String]) =>
          AlterTableUnsetPropertiesCommand(canAlter(id, "UNSET TBLPROPERTIES"), keys,
            exists.asInstanceOf[Option[Boolean]].isDefined, view))
    )
  }

  protected def alterTable: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ (
        (ADD ~ push(true) | DROP ~ push(false)) ~ (
            // other store ALTER statements which don't effect the snappydata catalog
            capture((PRIMARY | CONSTRAINT | CHECK | FOREIGN | UNIQUE) ~ ANY. +) ~>
                ((table: TableIdentifier, isAdd: Boolean, s: String) =>
                  AlterTableMiscCommand(table, s"ALTER TABLE ${quotedUppercaseId(table)} " +
                    s"${if (isAdd) "ADD" else "DROP"} $s")) |
            COLUMNS ~ ANY. + ~> ((_: TableIdentifier, _: Boolean) =>
              sparkParser.parsePlan(input.sliceString(0, input.length)))
        ) |
        ADD ~ COLUMN.? ~ column ~ capture(ANY.*) ~> AlterTableAddColumnCommand |
        DROP ~ COLUMN.? ~ identifier ~ capture(ANY.*) ~> AlterTableDropColumnCommand |
        // other store ALTER statements which don't effect the snappydata catalog
        capture((ALTER | SET) ~ ANY. +) ~> ((table: TableIdentifier, s: String) =>
          AlterTableMiscCommand(table, s"ALTER TABLE ${quotedUppercaseId(table)} $s")) |
        partitionSpec ~ CHANGE ~ ANY. + ~> ((_: TableIdentifier, _: Map[String, Option[String]]) =>
          sparkParser.parsePlan(input.sliceString(0, input.length)))
    )
  }

  protected def createStream: Rule1[LogicalPlan] = rule {
    CREATE ~ STREAM ~ TABLE ~ ifNotExists ~ tableIdentifier ~ tableSchema.? ~
        USING ~ qualifiedName ~ OPTIONS ~ options ~> {
      (allowExisting: Boolean, streamIdent: TableIdentifier, schema: Any,
          provider: String, opts: Map[String, String]) =>
        val specifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]]
            .map(fields => StructType(fields))
        // check that the provider is a stream relation
        val clazz = internals.lookupDataSource(provider, sqlConf)
        if (!classOf[StreamPlanProvider].isAssignableFrom(clazz)) {
          throw Utils.analysisException(s"CREATE STREAM provider $provider" +
              " does not implement StreamPlanProvider")
        }
        // provider has already been resolved, so isBuiltIn==false allows
        // for both builtin as well as external implementations
        val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTableUsingCommand(streamIdent, None, specifiedSchema, None,
          provider, mode, opts, properties = Map.empty, partitionColumns = Nil,
          bucketSpec = None, query = None, isExternal = false)
    }
  }

  protected final def resourceType: Rule1[FunctionResource] = rule {
    identifier ~ stringLiteral ~> { (rType: String, path: String) =>
      val resourceType = Utils.toLowerCase(rType)
      resourceType match {
        case "jar" =>
          FunctionResource(FunctionResourceType.fromString(resourceType), path)
        case _ =>
          throw Utils.analysisException(s"CREATE FUNCTION with resource type '$resourceType'")
      }
    }
  }

  protected def checkExists(resource: FunctionResource): Unit = {
    // TODO: SW: why only local "jar" type resources supported?
    if (!new File(resource.uri).exists()) {
      throw Utils.analysisException(s"No file named ${resource.uri} exists")
    }
  }

  /**
   * Create a [[CreateFunctionCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE [TEMPORARY] FUNCTION [db_name.]function_name AS class_name RETURNS ReturnType
   *    USING JAR 'file_uri';
   * }}}
   */
  protected def createFunction: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ (TEMPORARY ~ push(true)).? ~ FUNCTION ~
        ifNotExists ~ functionIdentifier ~ AS ~ (qualifiedName | stringLiteral) ~
        (RETURNS ~ columnDataType).? ~ USING ~ (resourceType + commaSep) ~>
        { (replace: Any, te: Any, ignoreIfExists: Boolean, functionIdent: FunctionIdentifier,
            className: String, t: Any, resources: Any) =>

          val isTemp = te.asInstanceOf[Option[Boolean]].isDefined
          val funcResources = resources.asInstanceOf[Seq[FunctionResource]]
          funcResources.foreach(checkExists)
          val catalogString = t.asInstanceOf[Option[DataType]] match {
            case None => ""
            case Some(CharType(Int.MaxValue)) | Some(VarcharType(Int.MaxValue)) => "string"
            case Some(dt) => dt.catalogString
          }
          val classNameWithType = className + "__" + catalogString
          internals.newCreateFunctionCommand(functionIdent.database,
            functionIdent.funcName, classNameWithType, funcResources, isTemp,
            ignoreIfExists, replace != None)
        }
  }

  /**
   * Create a [[DropFunctionCommand]] command.
   *
   * For example:
   * {{{
   *   DROP [TEMPORARY] FUNCTION [IF EXISTS] function;
   * }}}
   */
  protected def dropFunction: Rule1[LogicalPlan] = rule {
    DROP ~ (TEMPORARY ~ push(true)).? ~ FUNCTION ~ ifExists ~ functionIdentifier ~>
        ((te: Any, ifExists: Boolean, functionIdent: FunctionIdentifier) => DropFunctionCommand(
          functionIdent.database,
          functionIdent.funcName,
          ifExists = ifExists,
          isTemp = te.asInstanceOf[Option[Boolean]].isDefined))
  }

  /**
   * Commands like GRANT/REVOKE/CREATE DISKSTORE/CALL on a table that are passed through
   * as is to the SnappyData store layer (only for column and row tables).
   *
   * Example:
   * {{{
   *   GRANT SELECT ON table TO user1, user2;
   *   GRANT INSERT ON table TO ldapGroup: group1;
   *   CREATE DISKSTORE diskstore_name ('dir1' 10240)
   *   DROP DISKSTORE diskstore_name
   *   CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)
   * }}}
   */
  protected def passThrough: Rule1[LogicalPlan] = rule {
    (passThroughStatementInit | passThroughStatement(includeIndex = false)) ~>
        /* dummy table because we will pass sql to gemfire layer so we only need to have sql */
        (() => DMLExternalTable(LogicalRelation(new execution.row.DefaultSource().createRelation(
          session.sqlContext, Map(SnappyExternalCatalog.DBTABLE_PROPERTY -> JdbcExtendedUtils
              .DUMMY_TABLE_QUALIFIED_NAME))), input.sliceString(0, input.length)))
  }

  /**
   * SQL statements that are passed through to the store and need to be executed
   * at the very start for store `initialization`.
   */
  protected def passThroughStatementInit: Rule0 = rule {
    ((CREATE | DROP) ~ DISKSTORE | (('{' ~ ws).? ~ (CALL | EXECUTE))) ~ ANY.*
  }

  protected def passThroughStatement(includeIndex: Boolean): Rule0 = rule {
    (GRANT | REVOKE | (CREATE | DROP) ~ (TRIGGER | test(includeIndex) ~
        (GLOBAL ~ HASH | UNIQUE).? ~ INDEX)) ~ ANY.*
  }

  final def extTableIdentifier: Rule1[TableIdentifier] = rule {
    tableIdentifier ~> { (td: TableIdentifier) =>
      val tmd = session.sessionCatalog.getTableMetadata(td)
      if (tmd.tableType == CatalogTableType.EXTERNAL) push(td) else MISMATCH
    }
  }

  protected def grantRevokeExternal: Rule1[LogicalPlan] = rule {
    GRANT ~ ALL ~ ON ~ extTableIdentifier ~ TO ~ (groupOrUser + commaSep) ~>
        ((td: TableIdentifier, users: Seq[String]) =>
          GrantRevokeOnExternalTable(isGrant = true, td, users)) |
    REVOKE ~ ALL ~ ON ~ extTableIdentifier ~ FROM ~ (groupOrUser + commaSep) ~>
        ((td: TableIdentifier, users: Seq[String]) =>
          GrantRevokeOnExternalTable(isGrant = false, td, users))
  }

  /**
   * Handle other statements not appropriate for SnappyData's builtin sources but used by hive/file
   * based sources in Spark. This rule should always be at the end of the "start" rule so that
   * this is used as the last fallback and not before any of the SnappyData customizations.
   */
  protected def delegateToSpark: Rule1[LogicalPlan] = rule {
    (
        ADD | ANALYZE | ALTER ~ (DATABASE | TABLE | VIEW) | CREATE ~ DATABASE |
        DESCRIBE | DESC | DROP ~ DATABASE | LIST | LOAD | MSCK | REFRESH | SHOW | TRUNCATE
    ) ~ ANY.* ~>
        (() => sparkParser.parsePlan(input.sliceString(0, input.length)))
  }

  protected def deployPackages: Rule1[LogicalPlan] = rule {
    DEPLOY ~ ((PACKAGE ~ packageIdentifier ~ stringLiteral ~
        (REPOS ~ stringLiteral).? ~ (PATH ~ stringLiteral).? ~>
        ((alias: TableIdentifier, packages: String, repos: Any, path: Any) => DeployCommand(
          packages, alias.identifier, repos.asInstanceOf[Option[String]],
          path.asInstanceOf[Option[String]], restart = false))) |
      JAR ~ packageIdentifier ~ stringLiteral ~>
          ((alias: TableIdentifier, commaSepPaths: String) => DeployJarCommand(
        alias.identifier, commaSepPaths, restart = false))) |
    UNDEPLOY ~ packageIdentifier ~> ((alias: TableIdentifier) =>
      UndeployCommand(alias.identifier)) |
    LIST ~ (
      PACKAGES ~> (() => ListPackageJarsCommand(true)) |
      JARS ~> (() => ListPackageJarsCommand(false))
    )
  }

  protected def streamContext: Rule1[LogicalPlan] = rule {
    STREAMING ~ (
        INIT ~ durationUnit ~> ((batchInterval: Duration) =>
          SnappyStreamingActionsCommand(0, Some(batchInterval))) |
        START ~> (() => SnappyStreamingActionsCommand(1, None)) |
        STOP ~> (() => SnappyStreamingActionsCommand(2, None))
    )
  }

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,
   *   column_type,comment
   */
  protected def describe: Rule1[LogicalPlan] = rule {
    (DESCRIBE | DESC) ~ (
        FUNCTION ~ (EXTENDED ~ push(true)).? ~
            functionIdentifier ~> ((extended: Any, name: FunctionIdentifier) =>
          DescribeFunctionCommand(name,
            extended.asInstanceOf[Option[Boolean]].isDefined)) |
        DATABASE ~ (EXTENDED ~ push(true)).? ~ identifier ~>
            ((extended: Any, name: String) =>
              DescribeDatabaseCommand(name, extended.asInstanceOf[Option[Boolean]].isDefined)) |
        (EXTENDED ~ push(true) | FORMATTED ~ push(false)).? ~ tableIdentifier ~ partitionSpec ~>
            ((extendedOrFormatted: Any, tableIdent: TableIdentifier,
                spec: Map[String, Option[String]]) => {
              // ensure columns are sent back as CLOB for large results with EXTENDED
              allHints += QueryHint.ColumnsAsClob.name -> "data_type,comment"
              val (isExtended, isFormatted) = extendedOrFormatted match {
                case None => (false, false)
                case Some(true) => (true, false)
                case Some(false) => (false, true)
              }
              DescribeSnappyTableCommand(tableIdent,
                spec.mapValues(o => if (o.isEmpty) null else o.get), isExtended, isFormatted)
            })
    )
  }

  protected def refreshTable: Rule1[LogicalPlan] = rule {
    REFRESH ~ TABLE ~ tableIdentifier ~> RefreshTable
  }

  protected def cache: Rule1[LogicalPlan] = rule {
    CACHE ~ (LAZY ~ push(true)).? ~ TABLE ~ tableIdentifier ~
        (AS ~ query).? ~> ((isLazy: Any, tableIdent: TableIdentifier,
        plan: Any) => SnappyCacheTableCommand(tableIdent,
      input.sliceString(0, input.length), plan.asInstanceOf[Option[LogicalPlan]],
      isLazy.asInstanceOf[Option[Boolean]].isDefined))
  }

  protected def uncache: Rule1[LogicalPlan] = rule {
    UNCACHE ~ TABLE ~ ifExists ~ tableIdentifier ~>
        ((ifExists: Boolean, tableIdent: TableIdentifier) =>
          UncacheTableCommand(tableIdent, ifExists)) |
    CLEAR ~ CACHE ~> (() => internals.newClearCacheCommand())
  }

  protected def useDatabase: Rule1[LogicalPlan] = rule {
    SET ~ CURRENT.? ~ DATABASE ~ ('=' ~ ws).? ~ identifier ~> SetDatabaseCommand |
    USE ~ identifier ~> SetDatabaseCommand
  }

  protected def set: Rule1[LogicalPlan] = rule {
    SET ~ capture(ANY.*) ~> { (rest: String) =>
      val separatorIndex = rest.indexOf('=')
      if (separatorIndex >= 0) {
        val key = rest.substring(0, separatorIndex).trim
        val value = rest.substring(separatorIndex + 1).trim
        new SetSnappyCommand(Some(key -> Option(value)))
      } else if (rest.nonEmpty) {
        new SetSnappyCommand(Some(rest.trim -> None))
      } else {
        new SetSnappyCommand(None)
      }
    }
  }

  protected def reset: Rule1[LogicalPlan] = rule {
    RESET ~> { () => ResetCommand }
  }

  // helper non-terminals

  protected final def sortDirection: Rule1[SortDirection] = rule {
    ASC ~> (() => Ascending) | DESC ~> (() => Descending)
  }

  protected final def colsWithDirection: Rule1[ColumnDirectionMap] = rule {
    O_PAREN ~ (identifier ~ sortDirection.? ~> ((id: Any, direction: Any) =>
      (id, direction))).*(commaSep) ~ C_PAREN ~> ((cols: Any) =>
      cols.asInstanceOf[Seq[(String, Option[SortDirection])]])
  }

  protected final def durationUnit: Rule1[Duration] = rule {
    integral ~ (
        (MILLISECOND | MILLIS) ~> ((s: String) => Milliseconds(s.toInt)) |
        (SECOND | SECS) ~> ((s: String) => Seconds(s.toInt)) |
        (MINUTE | MINS) ~> ((s: String) => Minutes(s.toInt))
    )
  }

  /** the string passed in *SHOULD* be lower case */
  protected final def intervalUnit(k: String): Rule0 = rule {
    atomic(ignoreCase(k) ~ Consts.plural.?) ~ delimiter
  }

  protected final def intervalUnit(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower) ~ Consts.plural.?) ~ delimiter
  }

  protected final def qualifiedName: Rule1[String] = rule {
    ((unquotedIdentifier | quotedIdentifier) + ('.' ~ ws)) ~>
        ((ids: Seq[String]) => ids.mkString("."))
  }

  protected def column: Rule1[StructField] = rule {
    identifier ~ columnDataType ~ ((NOT ~ push(true)).? ~ NULL).? ~
        (COMMENT ~ stringLiteral).? ~> { (columnName: String,
        dt: DataType, notNull: Any, cm: Any) =>
      val builder = new MetadataBuilder()
      val (dataType, empty) = dt match {
        case CharType(size) =>
          builder.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
              .putString(Constant.CHAR_TYPE_BASE_PROP, "CHAR")
          (StringType, false)
        case VarcharType(Int.MaxValue) => // indicates CLOB type
          builder.putString(Constant.CHAR_TYPE_BASE_PROP, "CLOB")
          (StringType, false)
        case VarcharType(size) =>
          builder.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
              .putString(Constant.CHAR_TYPE_BASE_PROP, "VARCHAR")
          (StringType, false)
        case StringType =>
          builder.putString(Constant.CHAR_TYPE_BASE_PROP, "STRING")
          (StringType, false)
        case _ => (dt, true)
      }
      val metadata = cm.asInstanceOf[Option[String]] match {
        case Some(comment) => builder.putString(
          Consts.COMMENT.lower, comment).build()
        case None => if (empty) Metadata.empty else builder.build()
      }
      // Add Hive type string to metadata
      val cleanedDataType = HiveStringType.replaceCharType(dt)
      if (dt != cleanedDataType) {
        builder.putString(HIVE_TYPE_STRING, dt.catalogString)
      }

      val notNullOpt = notNull.asInstanceOf[Option[Option[Boolean]]]
      StructField(columnName, dataType, notNullOpt.isEmpty ||
          notNullOpt.get.isEmpty, metadata)
    }
  }

  protected final def tableSchema: Rule1[Seq[StructField]] = rule {
    O_PAREN ~ (column + commaSep) ~ C_PAREN
  }

  final def tableSchemaOpt: Rule1[Option[Seq[StructField]]] = rule {
    (ws ~ tableSchema ~> (Some(_)) | ws ~> (() => None)).named("tableSchema") ~ EOI
  }

  protected final def optionKey: Rule1[String] = rule {
    qualifiedName | stringLiteral
  }

  protected final def option: Rule1[(String, String)] = rule {
    optionKey ~ ('=' ~ '='.? ~ ws).? ~ (stringLiteral | numericLiteral | booleanLiteral) ~>
        ((k: String, v: Any) => k -> v.toString)
  }

  protected final def options: Rule1[Map[String, String]] = rule {
    O_PAREN ~ (option * commaSep) ~ C_PAREN ~>
        ((pairs: Any) => pairs.asInstanceOf[Seq[(String, String)]].toMap)
  }

  protected final def allowDDL: Rule0 = rule {
    test(!Misc.getGemFireCache.isSnappyRecoveryMode ||
        SnappyContext.getClusterMode(sparkContext).isInstanceOf[ThinClientConnectorMode])
  }

  protected def ddl: Rule1[LogicalPlan] = rule {
    describe | allowDDL ~ (createTableLike | createHiveTable | createTable | refreshTable |
    dropTable | truncateTable | createView | createTempViewUsing | dropView | alterView |
    createDatabase | dropDatabase | alterTableToggleRowLevelSecurity | createPolicy | dropPolicy |
    alterTableProps | alterTableOrView | alterTable | createStream | streamContext |
    createIndex | dropIndex | createFunction | dropFunction | grantRevokeInterpreter |
    grantRevokeExternal | passThrough | interpretCode)
  }

  protected def partitionSpec: Rule1[Map[String, Option[String]]]
  protected def query: Rule1[LogicalPlan]
  protected def expression: Rule1[Expression]
}

case class DMLExternalTable(child: LogicalPlan, command: String) extends UnaryNode {
  override lazy val resolved: Boolean = child.resolved
  override lazy val output: Seq[Attribute] = AttributeReference("count", IntegerType)() :: Nil
}
