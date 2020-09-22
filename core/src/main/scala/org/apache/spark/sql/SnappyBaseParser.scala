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

import java.util.concurrent.ConcurrentHashMap

import com.gemstone.gemfire.internal.shared.SystemProperties
import io.snappydata.QueryHint
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.set.mutable.UnifiedSet
import org.parboiled2._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, IdentifierWithDatabase, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils.{toLowerCase => lower, toUpperCase => upper}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}

/**
 * Base parsing facilities for all SnappyData SQL parsers.
 */
abstract class SnappyBaseParser extends Parser() {

  protected[sql] final var caseSensitive: Boolean = _

  protected final var escapedStringLiterals: Boolean = _

  protected final var sparkCompatible: Boolean = _

  protected final var quotedRegexColumnNames: Boolean = _

  protected final var legacySetOpsPrecedence: Boolean = _

  protected final var quotedFlags: Int = _

  /**
   * Disable hints processing within comments. This is done for two cases:
   * a) hints are plan level so they are explicitly processed to wrap the current LogicalPlan
   * b) expression parsing within value of hint will disable it to avoid nested hints
   */
  protected final var hintsEnabled: Boolean = true

  private[sql] final val queryHints: ConcurrentHashMap[String, String] =
    new ConcurrentHashMap[String, String](4, 0.7f, 1)

  protected final def disableHints: Boolean = {
    val v = hintsEnabled
    hintsEnabled = false
    v
  }

  protected final def hintStatement: Rule1[(String, String, Seq[Expression])] = rule {
    // disable possible nested hint processing
    push(disableHints) ~ identifier ~ capture((O_PAREN ~ (primaryExpression + commaSep) ~
        C_PAREN).?) ~> { (enabled: Boolean, name: String, args: Any, value: String) =>
      hintsEnabled = enabled
      val stringValue = if (value.isEmpty) value else value.substring(1, value.length - 2).trim
      // put all hints into the queryHints map including plan-level hints (helps plan caching
      // to determine whether or not to re-use the LogicalPlan that does not have physical
      // plan information that planHints effect)
      queryHints.put(name, stringValue)
      args.asInstanceOf[Option[Seq[Expression]]] match {
        case None => (name, "", Nil)
        case Some(s) => (name, stringValue, s)
      }
    }
  }

  protected final def commentBody: Rule0 = rule {
    "*/" | ANY ~ commentBody
  }

  protected final def commentBodyOrHint: Rule0 = rule {
    '+' ~ (hintStatement + commaSep) ~> (_ => ()) ~ commentBody |
    commentBody
  }

  protected final def lineCommentOrHint: Rule0 = rule {
    '+' ~ (hintStatement + commaSep) ~> (_ => ()) ~ noneOf(Consts.lineCommentEnd).* |
    noneOf(Consts.lineCommentEnd).*
  }

  /** The recognized whitespace characters and comments. */
  protected final def ws: Rule0 = rule {
    quiet(
      Consts.whitespace |
      '-' ~ '-' ~ (test(hintsEnabled) ~ lineCommentOrHint |
          test(!hintsEnabled) ~ !'+' ~ noneOf(Consts.lineCommentEnd).*) |
      '/' ~ '*' ~ (test(hintsEnabled) ~ (commentBodyOrHint | fail("unclosed comment")) |
          test(!hintsEnabled) ~ !'+' ~ (commentBody | fail("unclosed comment")))
    ).*
  }

  /** All recognized delimiters including whitespace. */
  final def delimiter: Rule0 = rule {
    quiet((Consts.whitespace ~ ws) | &(Consts.delimiters)) | EOI
  }

  protected final def commaSep: Rule0 = rule {
    ',' ~ ws
  }

  protected final def digits: Rule1[String] = rule {
    capture(CharPredicate.Digit. +) ~ ws
  }

  protected final def integral: Rule1[String] = rule {
    capture(Consts.plusOrMinus.? ~ CharPredicate.Digit. +) ~ ws
  }

  protected final def scientificNotation: Rule0 = rule {
    Consts.exponent ~ Consts.plusOrMinus.? ~ CharPredicate.Digit. +
  }

  protected final def stringLiteral: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    capture('\'' ~ (noneOf("'\\") | "''" | '\\' ~ ANY).* ~ '\'') ~ ws ~> { (s: String) =>
      if (s.indexOf("''") >= 0) {
        val str = s.substring(1, s.length - 1).replace("''", "\\'")
        if (escapedStringLiterals) str else ParserUtils.unescapeSQLString("'" + str + "'")
      }
      else if (escapedStringLiterals) s.substring(1, s.length - 1)
      else ParserUtils.unescapeSQLString(s)
    } |
    test(sparkCompatible) ~
        capture('"' ~ (noneOf("\"\\") | '\\' ~ ANY).* ~ '"') ~ ws ~> { (s: String) =>
      if (escapedStringLiterals) s.substring(1, s.length - 1) else ParserUtils.unescapeSQLString(s)
    }
  }

  protected def primaryExpression: Rule1[Expression]

  final def keyword(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower)) ~ delimiter
  }

  /**
   * Used for DataTypes. Not reserved and otherwise identical to "keyword"
   * apart from the name so as to appear properly in error messages related
   * to incorrect DataType definition.
   */
  protected final def newDataType(t: Keyword): Rule0 = rule {
    atomic(ignoreCase(t.lower)) ~ delimiter
  }

  final def sql: Rule1[LogicalPlan] = rule {
    ws ~ start ~ (';' ~ ws).* ~ EOI
  }

  protected def start: Rule1[LogicalPlan]

  protected final def unquotedIdentifier: Rule1[String] = rule {
    atomic(capture(Consts.identStart ~ Consts.identifier.*)) ~ delimiter
  }

  /** all unquoted identifiers which disallows reserved keywords */
  protected final def unreservedIdentifier: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    unquotedIdentifier ~> { (s: String) =>
      val lcase = lower(s)
      test(!Consts.reservedKeywords.contains(lcase)) ~
          push(if (caseSensitive) s else lcase)
    }
  }

  // noinspection ScalaUnnecessaryParentheses
  protected final def quotedIdentifier: Rule1[String] = rule {
    atomic('`' ~ capture((noneOf("`") | "``"). +) ~ '`') ~ ws ~> { (s: String) =>
      if (s.indexOf("``") >= 0) s.replace("``", "`") else s
    } |
    test(!sparkCompatible) ~
        atomic('"' ~ capture((noneOf("\"") | "\"\""). +) ~ '"') ~ ws ~> { (s: String) =>
      if (s.indexOf("\"\"") >= 0) s.replace("\"\"", "\"") else s
    }
  }

  protected final def identifier: Rule1[String] = rule {
    unreservedIdentifier | quotedIdentifier
  }

  protected final def identifierWithQuotedFlag: Rule1[String] = rule {
    unreservedIdentifier ~> (() => quotedFlags = quotedFlags << 1) |
    quotedIdentifier ~> (() => quotedFlags = (quotedFlags << 1) | 0x1)
  }

  /**
   * A strictIdentifier is more restricted than an identifier in that neither
   * any of the SQL reserved keywords nor non-reserved keywords will be
   * interpreted as a strictIdentifier.
   */
  protected final def strictIdentifier: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    unquotedIdentifier ~> { (s: String) =>
      val lcase = lower(s)
      test(!Consts.allKeywords.contains(lcase)) ~
          push(if (caseSensitive) s else lcase)
    } |
    quotedIdentifier
  }

  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  protected final def quotedUppercaseId(id: IdentifierWithDatabase): String = id.database match {
    case None => s"`${upper(quoteIdentifier(id.identifier))}`"
    case Some(d) => s"`${upper(quoteIdentifier(d))}`.`${upper(quoteIdentifier(id.identifier))}`"
  }

  protected final def O_PAREN: Rule0 = rule {
    ch('(') ~ ws
  }

  protected final def C_PAREN: Rule0 = rule {
    ch(')') ~ ws
  }

  // DataTypes
  // It is not useful to see long list of "expected ARRAY or BIGINT or ..."
  // for parse errors, so not making these separate rules and instead naming
  // the common rule as "datatype" which is otherwise identical to "keyword"
  final def ARRAY: Rule0 = newDataType(Consts.ARRAY)
  final def BIGINT: Rule0 = newDataType(Consts.BIGINT)
  final def BINARY: Rule0 = newDataType(Consts.BINARY)
  final def BLOB: Rule0 = newDataType(Consts.BLOB)
  final def BOOLEAN: Rule0 = newDataType(Consts.BOOLEAN)
  final def BYTE: Rule0 = newDataType(Consts.BYTE)
  final def CHAR: Rule0 = newDataType(Consts.CHAR)
  final def CLOB: Rule0 = newDataType(Consts.CLOB)
  final def DATE: Rule0 = newDataType(Consts.DATE)
  final def DECIMAL: Rule0 = newDataType(Consts.DECIMAL)
  final def DOUBLE: Rule0 = newDataType(Consts.DOUBLE)
  final def FLOAT: Rule0 = newDataType(Consts.FLOAT)
  final def INT: Rule0 = newDataType(Consts.INT)
  final def INTEGER: Rule0 = newDataType(Consts.INTEGER)
  final def LONG: Rule0 = newDataType(Consts.LONG)
  final def MAP: Rule0 = newDataType(Consts.MAP)
  final def NUMERIC: Rule0 = newDataType(Consts.NUMERIC)
  final def PRECISION: Rule0 = keyword(Consts.PRECISION)
  final def REAL: Rule0 = newDataType(Consts.REAL)
  final def SHORT: Rule0 = newDataType(Consts.SHORT)
  final def SMALLINT: Rule0 = newDataType(Consts.SMALLINT)
  final def STRING: Rule0 = newDataType(Consts.STRING)
  final def STRUCT: Rule0 = newDataType(Consts.STRUCT)
  final def TIMESTAMP: Rule0 = newDataType(Consts.TIMESTAMP)
  final def TINYINT: Rule0 = newDataType(Consts.TINYINT)
  final def VARBINARY: Rule0 = newDataType(Consts.VARBINARY)
  final def VARCHAR: Rule0 = newDataType(Consts.VARCHAR)

  protected final def fixedDecimalType: Rule1[DataType] = rule {
    (DECIMAL | NUMERIC) ~ O_PAREN ~ digits ~ (commaSep ~ digits).? ~ C_PAREN ~>
        ((precision: String, scale: Any) => DecimalType(precision.toInt,
          if (scale == None) 0 else scale.asInstanceOf[Option[String]].get.toInt))
  }

  protected final def primitiveType: Rule1[DataType] = rule {
    STRING ~> (() => StringType) |
    INTEGER ~> (() => IntegerType) |
    INT ~> (() => IntegerType) |
    BIGINT ~> (() => LongType) |
    LONG ~> (() => LongType) |
    DOUBLE ~ PRECISION.? ~> (() => DoubleType) |
    fixedDecimalType |
    DECIMAL ~> (() => DecimalType.SYSTEM_DEFAULT) |
    NUMERIC ~> (() => DecimalType.SYSTEM_DEFAULT) |
    DATE ~> (() => DateType) |
    TIMESTAMP ~> (() => TimestampType) |
    FLOAT ~> (() => FloatType) |
    REAL ~> (() => FloatType) |
    BOOLEAN ~> (() => BooleanType) |
    CLOB ~> (() => StringType) |
    BLOB ~> (() => BinaryType) |
    BINARY ~> (() => BinaryType) |
    VARBINARY ~> (() => BinaryType) |
    SMALLINT ~> (() => ShortType) |
    SHORT ~> (() => ShortType) |
    TINYINT ~> (() => ByteType) |
    BYTE ~> (() => ByteType)
  }

  protected final def charType: Rule1[DataType] = rule {
    VARCHAR ~ O_PAREN ~ digits ~ C_PAREN ~> ((_: String) => StringType) |
    CHAR ~ O_PAREN ~ digits ~ C_PAREN ~> ((_: String) => StringType)
  }

  final def dataType: Rule1[DataType] = rule {
    charType | primitiveType | arrayType | mapType | structType
  }

  protected final def arrayType: Rule1[DataType] = rule {
    ARRAY ~ '<' ~ ws ~ dataType ~ '>' ~ ws ~>
        ((t: DataType) => ArrayType(t))
  }

  protected final def mapType: Rule1[DataType] = rule {
    MAP ~ '<' ~ ws ~ dataType ~ commaSep ~ dataType ~ '>' ~ ws ~>
        ((t1: DataType, t2: DataType) => MapType(t1, t2))
  }

  protected final def structField: Rule1[StructField] = rule {
    identifier ~ ':' ~ ws ~ dataType ~> ((name: String, t: DataType) => StructField(name, t))
  }

  protected final def structType: Rule1[DataType] = rule {
    STRUCT ~ '<' ~ ws ~ (structField * commaSep) ~ '>' ~ ws ~>
        ((f: Any) => StructType(f.asInstanceOf[Seq[StructField]].toArray))
  }

  protected final def columnCharType: Rule1[DataType] = rule {
    VARCHAR ~ O_PAREN ~ digits ~ C_PAREN ~> ((d: String) => VarcharType(d.toInt)) |
    CHAR ~ O_PAREN ~ digits ~ C_PAREN ~> ((d: String) => CharType(d.toInt)) |
    STRING ~> (() => StringType) |
    CLOB ~> (() => VarcharType(Int.MaxValue))
  }

  final def columnDataType: Rule1[DataType] = rule {
    columnCharType | primitiveType | arrayType | mapType | structType
  }

  /** allow for first character of unquoted identifier to be a numeric */
  protected final def identifierExt1: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    atomic(capture(CharPredicate.Digit.* ~ Consts.identStart ~ Consts.identifier.*)) ~
        delimiter ~> { (s: String) =>
      val lcase = lower(s)
      test(!Consts.reservedKeywords.contains(lcase)) ~
          push(if (caseSensitive) s else lcase)
    } |
    quotedIdentifier
  }

  /** first character of unquoted identifier can be a numeric and also allows reserved keywords */
  protected final def identifierExt2: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    atomic(capture(CharPredicate.Digit.* ~ Consts.identStart ~ Consts.identifier.*)) ~ delimiter ~>
        ((s: String) => if (caseSensitive) s else lower(s)) |
    quotedIdentifier
  }

  protected final def packageIdentifierPart: Rule1[String] = rule {
    // noinspection ScalaUnnecessaryParentheses
    atomic(capture(Consts.packageIdentifier. +)) ~ ws ~> { (s: String) =>
      val lcase = lower(s)
      test(!Consts.reservedKeywords.contains(lcase)) ~
          push(if (caseSensitive) s else lcase)
    } |
        quotedIdentifier
  }

  final def tableIdentifier: Rule1[TableIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifierExt"
    (identifierExt1 ~ '.' ~ ws).? ~ identifierExt1 ~> ((schema: Any, table: String) =>
      TableIdentifier(table, schema.asInstanceOf[Option[String]]))
  }

  final def packageIdentifier: Rule1[TableIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifierExt1 ~ '.' ~ ws).? ~ packageIdentifierPart ~> ((schema: Any, table: String) =>
      TableIdentifier(table, schema.asInstanceOf[Option[String]]))
  }

  final def functionIdentifier: Rule1[FunctionIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifier ~ '.' ~ ws).? ~ identifier ~> ((schema: Any, name: String) =>
      FunctionIdentifier(name, database = schema.asInstanceOf[Option[String]]))
  }
}

final class Keyword private[sql](s: String) {
  val lower: String = Utils.toLowerCase(s)

  override def hashCode(): Int = lower.hashCode

  override def equals(obj: Any): Boolean = {
    this.eq(obj.asInstanceOf[AnyRef]) ||
        (obj.isInstanceOf[Keyword] && lower == obj.asInstanceOf[Keyword].lower)
  }

  override def toString: String = lower
}

final class ParseException(msg: String, cause: Option[Throwable] = None)
    extends AnalysisException(msg, None, None, None, cause)

object SnappyParserConsts {
  final val space: CharPredicate = CharPredicate(' ', '\t')
  final val whitespace: CharPredicate = CharPredicate(
    ' ', '\t', '\n', '\r', '\f')
  final val delimiters: CharPredicate = CharPredicate('@', '*',
    '+', '-', '<', '=', '!', '>', '/', '(', ')', ',', ';', '%', '{', '}', ':',
    '[', ']', '.', '&', '|', '^', '~', '#')
  final val lineCommentEnd: String = "\n\r\f" + EOI
  final val underscore: CharPredicate = CharPredicate('_')
  final val identifier: CharPredicate = CharPredicate.AlphaNum ++ underscore
  final val identStart: CharPredicate = CharPredicate.Alpha ++ underscore
  final val packageIdentifier: CharPredicate = identifier ++ CharPredicate('-', '.')
  final val plusOrMinus: CharPredicate = CharPredicate('+', '-')
  final val unaryOperator: CharPredicate = CharPredicate('+', '-', '~')
  /** arithmetic operators having the same precedence as multiplication */
  final val multPrecOperator = CharPredicate('*', '/', '%')
  /**
   * arithmetic operators having the same precedence as addition
   * except '|' which is handled separately
   */
  final val sumPrecOperator = CharPredicate('+', '-', '&', '^')
  final val exponent: CharPredicate = CharPredicate('e', 'E')
  final val numeric: CharPredicate = CharPredicate.Digit ++
      CharPredicate('.')
  final val numericSuffix: CharPredicate =
    CharPredicate('D', 'd', 'F', 'f', 'L', 'l', 'B', 'b', 'S', 's', 'Y', 'y')
  final val plural: CharPredicate = CharPredicate('s', 'S')

  final val reservedKeywords: UnifiedSet[String] = new UnifiedSet[String]

  final val allKeywords: UnifiedSet[String] = new UnifiedSet[String]

  final val optimizableLikePattern: java.util.regex.Pattern =
    java.util.regex.Pattern.compile("(%?[^_%]*[^_%\\\\]%?)|([^_%]*[^_%\\\\]%[^_%]*)")

  /**
   * Define the hints that need to be applied at plan-level and will be
   * wrapped by LogicalPlan
   */
  final val allowedPlanHints: Array[QueryHint.Type] = Array(QueryHint.JoinType)

  // -10 in sequence will mean all arguments, -1 will mean all odd argument and
  // -2 will mean all even arguments. -3 will mean all arguments except those listed after it.
  // Empty argument array means plan caching has to be disabled. Indexes are 0-based.
  final val FOLDABLE_FUNCTIONS: UnifiedMap[String, Array[Int]] = Utils.toOpenHashMap(Map(
    "round" -> Array(1), "bround" -> Array(1), "percentile" -> Array(1), "stack" -> Array(0),
    "ntile" -> Array(0), "str_to_map" -> Array(1, 2), "named_struct" -> Array(-2),
    "reflect" -> Array(0, 1), "java_method" -> Array(0, 1), "xpath" -> Array(1),
    "xpath_boolean" -> Array(1), "xpath_double" -> Array(1),
    "xpath_number" -> Array(1), "xpath_float" -> Array(1),
    "xpath_int" -> Array(1), "xpath_long" -> Array(1),
    "xpath_short" -> Array(1), "xpath_string" -> Array(1),
    "percentile_approx" -> Array(1, 2), "approx_percentile" -> Array(1, 2),
    "translate" -> Array(1, 2), "unix_timestamp" -> Array(1),
    "to_unix_timestamp" -> Array(1), "from_unix_timestamp" -> Array(1),
    "to_utc_timestamp" -> Array(1), "from_utc_timestamp" -> Array(1),
    "from_unixtime" -> Array(1), "trunc" -> Array(1), "next_day" -> Array(1),
    "get_json_object" -> Array(1), "json_tuple" -> Array(-3, 0),
    "first" -> Array(1), "last" -> Array(1),
    "window" -> Array(1, 2, 3), "rand" -> Array(0), "randn" -> Array(0),
    "parse_url" -> Array(0, 1, 2),
    "lag" -> Array(1), "lead" -> Array(1),
    // rand() plans are not to be cached since each run should use different seed
    // and the Spark impls create the seed in constructor rather than in generated code
    "rand" -> Array.emptyIntArray, "randn" -> Array.emptyIntArray,
    "like" -> Array(1), "rlike" -> Array(1), "approx_count_distinct" -> Array(1)))

  /**
   * Registering a Keyword with this method marks it a reserved keyword,
   * i.e. it is interpreted as a keyword wherever it may appear and is never
   * interpreted as an identifier (except if quoted).
   * <p>
   * Use this only for SQL reserved keywords.
   */
  private[sql] def reservedKeyword(s: String): Keyword = {
    val k = new Keyword(s)
    reservedKeywords.add(k.lower)
    allKeywords.add(k.lower)
    k
  }

  /**
   * Registering a Keyword with this method marks it as non-reserved.
   * These can be interpreted as identifiers as per the parsing rules,
   * but never interpreted as a "strictIdentifier" when `isKeyword` argument is true.
   * In other words, use "strictIdentifier" in parsing rules where there can be an ambiguity
   * between an identifier and a non-reserved keyword (e.g. LIMIT).
   * <p>
   * Use this for all SQL keywords used by grammar that are not reserved.
   * If the second argument `isKeyword` is false, then it can be freely
   * used in alias names and will only be interpreted as having special meaning when used
   * in the specific context (e.g. DEPLOY is a statement by itself at the start of SQL
   * but can also be used as an alias anywhere).
   */
  private[sql] def nonReserved(s: String, isKeyword: Boolean = true): Keyword = {
    val k = new Keyword(s)
    if (isKeyword) allKeywords.add(k.lower)
    k
  }

  final val COLUMN_SOURCE = "column"
  final val ROW_SOURCE = "row"
  final val DEFAULT_SOURCE = ROW_SOURCE

  // reserved keywords
  final val ALL: Keyword = reservedKeyword("all")
  final val AND: Keyword = reservedKeyword("and")
  final val AS: Keyword = reservedKeyword("as")
  final val CASE: Keyword = reservedKeyword("case")
  final val CAST: Keyword = reservedKeyword("cast")
  final val CREATE: Keyword = reservedKeyword("create")
  final val DISTINCT: Keyword = reservedKeyword("distinct")
  final val ELSE: Keyword = reservedKeyword("else")
  final val EXCEPT: Keyword = reservedKeyword("except")
  final val FALSE: Keyword = reservedKeyword("false")
  final val FROM: Keyword = reservedKeyword("from")
  final val GROUP: Keyword = reservedKeyword("group")
  final val HAVING: Keyword = reservedKeyword("having")
  final val IN: Keyword = reservedKeyword("in")
  final val INNER: Keyword = reservedKeyword("inner")
  final val INTERSECT: Keyword = reservedKeyword("intersect")
  final val INTO: Keyword = reservedKeyword("into")
  final val IS: Keyword = reservedKeyword("is")
  final val JOIN: Keyword = reservedKeyword("join")
  final val LEFT: Keyword = reservedKeyword("left")
  final val NOT: Keyword = reservedKeyword("not")
  final val NULL: Keyword = reservedKeyword("null")
  final val ON: Keyword = reservedKeyword("on")
  final val OR: Keyword = reservedKeyword("or")
  final val ORDER: Keyword = reservedKeyword("order")
  final val OUTER: Keyword = reservedKeyword("outer")
  final val RIGHT: Keyword = reservedKeyword("right")
  final val SELECT: Keyword = reservedKeyword("select")
  final val TABLE: Keyword = reservedKeyword("table")
  final val THEN: Keyword = reservedKeyword("then")
  final val TO: Keyword = reservedKeyword("to")
  final val TRUE: Keyword = reservedKeyword("true")
  final val UNION: Keyword = reservedKeyword("union")
  final val USING: Keyword = reservedKeyword("using")
  final val WHEN: Keyword = reservedKeyword("when")
  final val WHERE: Keyword = reservedKeyword("where")
  final val WITH: Keyword = reservedKeyword("with")

  // marked as internal keywords as reserved to prevent use in SQL
  final val HIVE_METASTORE: Keyword = reservedKeyword(SystemProperties.SNAPPY_HIVE_METASTORE)
  final val SAMPLER_WEIGHTAGE: Keyword = reservedKeyword(Utils.WEIGHTAGE_COLUMN_NAME)

  // non-reserved keywords that can be freely used in alias names
  final val ADD: Keyword = nonReserved("add")
  final val ALTER: Keyword = nonReserved("alter")
  final val ANALYZE: Keyword = nonReserved("analyze", isKeyword = false)
  final val ANTI: Keyword = nonReserved("anti")
  final val ASC: Keyword = nonReserved("asc")
  final val AUTHORIZATION: Keyword = nonReserved("authorization")
  final val BETWEEN: Keyword = nonReserved("between")
  final val BOTH: Keyword = nonReserved("both", isKeyword = false)
  final val BUCKETS: Keyword = nonReserved("buckets", isKeyword = false)
  final val BY: Keyword = nonReserved("by")
  final val CACHE: Keyword = nonReserved("cache", isKeyword = false)
  final val CALL: Keyword = nonReserved("call")
  final val CASCADE: Keyword = nonReserved("cascade", isKeyword = false)
  final val CHANGE: Keyword = nonReserved("change", isKeyword = false)
  final val CHECK: Keyword = nonReserved("check", isKeyword = false)
  final val CLEAR: Keyword = nonReserved("clear")
  final val CLUSTER: Keyword = nonReserved("cluster", isKeyword = false)
  final val CLUSTERED: Keyword = nonReserved("clustered", isKeyword = false)
  final val CODEGEN: Keyword = nonReserved("codegen", isKeyword = false)
  final val COLUMN: Keyword = nonReserved("column")
  final val COLUMNS: Keyword = nonReserved("columns", isKeyword = false)
  final val COMMENT: Keyword = nonReserved("comment")
  final val COMPUTE: Keyword = nonReserved("compute", isKeyword = false)
  final val CONSTRAINT: Keyword = nonReserved("constraint", isKeyword = false)
  final val COST: Keyword = nonReserved("cost", isKeyword = false)
  final val CROSS: Keyword = nonReserved("cross")
  final val CUBE: Keyword = nonReserved("cube")
  final val CURRENT: Keyword = nonReserved("current")
  final val CURRENT_DATE: Keyword = nonReserved("current_date")
  final val CURRENT_TIMESTAMP: Keyword = nonReserved("current_timestamp")
  final val CURRENT_USER: Keyword = nonReserved("current_user")
  final val DATABASE: Keyword = nonReserved("database", isKeyword = false)
  final val DATABASES: Keyword = nonReserved("databases", isKeyword = false)
  final val DELETE: Keyword = nonReserved("delete")
  final val DEPLOY: Keyword = nonReserved("deploy", isKeyword = false)
  final val DESC: Keyword = nonReserved("desc")
  final val DESCRIBE: Keyword = nonReserved("describe")
  final val DIRECTORY: Keyword = nonReserved("directory", isKeyword = false)
  final val DISABLE: Keyword = nonReserved("disable")
  final val DISKSTORE: Keyword = nonReserved("diskstore", isKeyword = false)
  final val DISTRIBUTE: Keyword = nonReserved("distribute")
  final val DIV: Keyword = nonReserved("div", isKeyword = false)
  final val DROP: Keyword = nonReserved("drop")
  final val DURATION: Keyword = nonReserved("duration")
  final val ENABLE: Keyword = nonReserved("enable")
  final val END: Keyword = nonReserved("end")
  final val EXECUTE: Keyword = nonReserved("execute")
  final val EXISTS: Keyword = nonReserved("exists")
  final val EXPLAIN: Keyword = nonReserved("explain")
  final val EXTENDED: Keyword = nonReserved("extended")
  final val EXTERNAL: Keyword = nonReserved("external")
  final val EXTRACT: Keyword = nonReserved("extract", isKeyword = false)
  final val FETCH: Keyword = nonReserved("fetch")
  final val FIRST: Keyword = nonReserved("first")
  final val FN: Keyword = nonReserved("fn")
  final val FOLLOWING: Keyword = nonReserved("following")
  final val FOR: Keyword = nonReserved("for")
  final val FOREIGN: Keyword = nonReserved("foreign", isKeyword = false)
  final val FORMAT: Keyword = nonReserved("format", isKeyword = false)
  final val FORMATTED: Keyword = nonReserved("formatted", isKeyword = false)
  final val FULL: Keyword = nonReserved("full")
  final val FUNCTION: Keyword = nonReserved("function")
  final val FUNCTIONS: Keyword = nonReserved("functions")
  final val GLOBAL: Keyword = nonReserved("global", isKeyword = false)
  final val GRANT: Keyword = nonReserved("grant")
  final val GROUPING: Keyword = nonReserved("grouping")
  final val HASH: Keyword = nonReserved("hash", isKeyword = false)
  final val IF: Keyword = nonReserved("if")
  final val IGNORE: Keyword = nonReserved("ignore", isKeyword = false)
  final val INDEX: Keyword = nonReserved("index")
  final val INIT: Keyword = nonReserved("init", isKeyword = false)
  final val INSERT: Keyword = nonReserved("insert")
  final val INTERVAL: Keyword = nonReserved("interval")
  final val JAR: Keyword = nonReserved("jar", isKeyword = false)
  final val JARS: Keyword = nonReserved("jars", isKeyword = false)
  final val LAST: Keyword = nonReserved("last")
  final val LATERAL: Keyword = nonReserved("lateral")
  final val LAZY: Keyword = nonReserved("lazy", isKeyword = false)
  final val LDAPGROUP: Keyword = nonReserved("ldapgroup", isKeyword = false)
  final val LEADING: Keyword = nonReserved("leading", isKeyword = false)
  final val LEVEL: Keyword = nonReserved("level", isKeyword = false)
  final val LIKE: Keyword = nonReserved("like")
  final val LIMIT: Keyword = nonReserved("limit")
  final val LIST: Keyword = nonReserved("list", isKeyword = false)
  final val LOAD: Keyword = nonReserved("load", isKeyword = false)
  final val LOCAL: Keyword = nonReserved("local", isKeyword = false)
  final val LOCATION: Keyword = nonReserved("location", isKeyword = false)
  final val MEMBERS: Keyword = nonReserved("members", isKeyword = false)
  final val MSCK: Keyword = nonReserved("msck", isKeyword = false)
  final val NATURAL: Keyword = nonReserved("natural")
  final val NULLS: Keyword = nonReserved("nulls")
  final val OF: Keyword = nonReserved("of", isKeyword = false)
  final val ONLY: Keyword = nonReserved("only")
  final val OPTIONS: Keyword = nonReserved("options")
  final val OUT: Keyword = nonReserved("out", isKeyword = false)
  final val OVER: Keyword = nonReserved("over")
  final val OVERWRITE: Keyword = nonReserved("overwrite")
  final val PACKAGE: Keyword = nonReserved("package", isKeyword = false)
  final val PACKAGES: Keyword = nonReserved("packages", isKeyword = false)
  final val PATH: Keyword = nonReserved("path", isKeyword = false)
  final val PARTITION: Keyword = nonReserved("partition", isKeyword = false)
  final val PARTITIONED: Keyword = nonReserved("partitioned", isKeyword = false)
  final val PERCENT: Keyword = nonReserved("percent", isKeyword = false)
  final val PIVOT: Keyword = nonReserved("pivot")
  final val POLICY: Keyword = nonReserved("policy", isKeyword = false)
  final val POSITION: Keyword = nonReserved("position", isKeyword = false)
  final val PRECEDING: Keyword = nonReserved("preceding")
  final val PRIMARY: Keyword = nonReserved("primary", isKeyword = false)
  final val PURGE: Keyword = nonReserved("purge", isKeyword = false)
  final val PUT: Keyword = nonReserved("put", isKeyword = false)
  final val RANGE: Keyword = nonReserved("range")
  final val REFRESH: Keyword = nonReserved("refresh", isKeyword = false)
  final val REGEXP: Keyword = nonReserved("regexp")
  final val RENAME: Keyword = nonReserved("rename")
  final val REPLACE: Keyword = nonReserved("replace")
  final val REPOS: Keyword = nonReserved("repos", isKeyword = false)
  final val REVOKE: Keyword = nonReserved("revoke")
  final val RESET: Keyword = nonReserved("reset")
  final val RESTRICT: Keyword = nonReserved("restrict")
  final val RETURNS: Keyword = nonReserved("returns", isKeyword = false)
  final val RLIKE: Keyword = nonReserved("rlike")
  final val ROLLUP: Keyword = nonReserved("rollup")
  final val ROW: Keyword = nonReserved("row")
  final val ROWS: Keyword = nonReserved("rows")
  final val SCHEMA: Keyword = nonReserved("schema")
  final val SCHEMAS: Keyword = nonReserved("schemas")
  final val SECURITY: Keyword = nonReserved("security", isKeyword = false)
  final val SEMI: Keyword = nonReserved("semi")
  final val SERDE: Keyword = nonReserved("serde", isKeyword = false)
  final val SERDEPROPERTIES: Keyword = nonReserved("serdeproperties", isKeyword = false)
  final val SET: Keyword = nonReserved("set")
  final val SETMINUS: Keyword = nonReserved("minus")
  final val SETS: Keyword = nonReserved("sets")
  final val SHOW: Keyword = nonReserved("show")
  final val SKEWED: Keyword = nonReserved("skewed", isKeyword = false)
  final val SLIDE: Keyword = nonReserved("slide")
  final val SORT: Keyword = nonReserved("sort")
  final val SORTED: Keyword = nonReserved("sorted", isKeyword = false)
  final val STATISTICS: Keyword = nonReserved("statistics", isKeyword = false)
  final val START: Keyword = nonReserved("start")
  final val STOP: Keyword = nonReserved("stop")
  final val STORED: Keyword = nonReserved("stored", isKeyword = false)
  final val STREAM: Keyword = nonReserved("stream", isKeyword = false)
  final val STREAMING: Keyword = nonReserved("streaming", isKeyword = false)
  final val TABLES: Keyword = nonReserved("tables")
  final val TABLESAMPLE: Keyword = nonReserved("tablesample", isKeyword = false)
  final val TBLPROPERTIES: Keyword = nonReserved("tblproperties", isKeyword = false)
  final val TEMP: Keyword = nonReserved("temp", isKeyword = false)
  final val TEMPORARY: Keyword = nonReserved("temporary")
  final val TRAILING: Keyword = nonReserved("trailing", isKeyword = false)
  final val TRIGGER: Keyword = nonReserved("trigger", isKeyword = false)
  final val TRIM: Keyword = nonReserved("trim", isKeyword = false)
  final val TRUNCATE: Keyword = nonReserved("truncate")
  final val UNBOUNDED: Keyword = nonReserved("unbounded")
  final val UNCACHE: Keyword = nonReserved("uncache", isKeyword = false)
  final val UNDEPLOY: Keyword = nonReserved("undeploy", isKeyword = false)
  final val UNIQUE: Keyword = nonReserved("unique", isKeyword = false)
  final val UNSET: Keyword = nonReserved("unset", isKeyword = false)
  final val UPDATE: Keyword = nonReserved("update")
  final val USE: Keyword = nonReserved("use")
  final val USER: Keyword = nonReserved("user")
  final val VALUES: Keyword = nonReserved("values")
  final val VIEW: Keyword = nonReserved("view")
  final val VIEWS: Keyword = nonReserved("views")
  final val WINDOW: Keyword = nonReserved("window")

  // interval units are not reserved and can be freely used in `strictIdentifier`
  final val DAY: Keyword = nonReserved("day", isKeyword = false)
  final val HOUR: Keyword = nonReserved("hour", isKeyword = false)
  final val MICROSECOND: Keyword = nonReserved("microsecond", isKeyword = false)
  final val MILLISECOND: Keyword = nonReserved("millisecond", isKeyword = false)
  final val MINUTE: Keyword = nonReserved("minute", isKeyword = false)
  final val MONTH: Keyword = nonReserved("month", isKeyword = false)
  final val SECOND: Keyword = nonReserved("second", isKeyword = false)
  final val WEEK: Keyword = nonReserved("week", isKeyword = false)
  final val YEAR: Keyword = nonReserved("year", isKeyword = false)

  // datatypes are not reserved
  final val ARRAY: Keyword = nonReserved("array")
  final val BIGINT: Keyword = nonReserved("bigint")
  final val BINARY: Keyword = nonReserved("binary")
  final val BLOB: Keyword = nonReserved("blob")
  final val BOOLEAN: Keyword = nonReserved("boolean")
  final val BYTE: Keyword = nonReserved("byte")
  final val CHAR: Keyword = nonReserved("char")
  final val CLOB: Keyword = nonReserved("clob")
  final val DATE: Keyword = nonReserved("date")
  final val DECIMAL: Keyword = nonReserved("decimal")
  final val DOUBLE: Keyword = nonReserved("double")
  final val FLOAT: Keyword = nonReserved("float")
  final val INT: Keyword = nonReserved("int")
  final val INTEGER: Keyword = nonReserved("integer")
  final val LONG: Keyword = nonReserved("long")
  final val MAP: Keyword = nonReserved("map")
  final val NUMERIC: Keyword = nonReserved("numeric")
  final val PRECISION: Keyword = nonReserved("precision")
  final val REAL: Keyword = nonReserved("real")
  final val SHORT: Keyword = nonReserved("short")
  final val SMALLINT: Keyword = nonReserved("smallint")
  final val STRING: Keyword = nonReserved("string")
  final val STRUCT: Keyword = nonReserved("struct")
  final val TIMESTAMP: Keyword = nonReserved("timestamp")
  final val TINYINT: Keyword = nonReserved("tinyint")
  final val VARBINARY: Keyword = nonReserved("varbinary")
  final val VARCHAR: Keyword = nonReserved("varchar")

  // for AQP
  final val ERROR: Keyword = nonReserved("error")
  final val ESTIMATE: Keyword = nonReserved("estimate")
  final val CONFIDENCE: Keyword = nonReserved("confidence")
  final val BEHAVIOR: Keyword = nonReserved("behavior")
  final val SAMPLE: Keyword = nonReserved("sample")
  final val TOPK: Keyword = nonReserved("topk")
}
