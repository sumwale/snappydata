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

import java.util.function.BiConsumer
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.google.common.primitives.Ints
import io.snappydata.{Property, QueryHint}
import org.eclipse.collections.impl.stack.mutable.primitive.BooleanArrayStack
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.{ShowSnappyTablesCommand, ShowViewsCommand}
import org.apache.spark.sql.internal.LikeEscapeSimplification
import org.apache.spark.sql.sources.{Delete, DeleteFromTable, PutIntoTable, Update}
import org.apache.spark.sql.streaming.WindowLogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming.Duration
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.random.RandomSampler

class SnappyParser(session: SnappySession)
    extends SnappyDDLParser(session) with ParamLiteralHolder {

  private[this] final var _input: ParserInput = _

  /** counter to deal with nested SELECTs to reset [[_inProjection]] after end of inner SELECT */
  private[this] final var _selectContext: Int = _
  /** set to true if parser is within the projection portion of a SELECT statement */
  private[this] final var _inProjection: Boolean = _

  protected final var _questionMarkCounter: Int = _
  protected final var _isPreparePhase: Boolean = _
  protected final var _parameterValueSet: Option[_] = None
  protected final var _fromRelations: mutable.Stack[LogicalPlan] = new mutable.Stack[LogicalPlan]
  // type info for parameters of a prepared statement
  protected final var _preparedParamsTypesInfo: Option[Array[Int]] = None

  override final def input: ParserInput = _input

  final def questionMarkCounter: Int = _questionMarkCounter

  private[sql] final def input_=(in: ParserInput): Unit = {
    clearQueryHints()
    _input = in
    clearConstants()
    _questionMarkCounter = 0
    cacheLiteral = false
  }

  private[sql] final def isPreparePhase: Boolean = _isPreparePhase

  private[sql] final def setPreparedQuery(preparePhase: Boolean, paramSet: Option[_]): Unit = {
    _isPreparePhase = preparePhase
    _parameterValueSet = paramSet
    if (preparePhase) _preparedParamsTypesInfo = None
  }

  private[sql] final def setPrepareParamsTypesInfo(info: Array[Int]): Unit = {
    _preparedParamsTypesInfo = Option(info)
  }

  private def toDecimalLiteral(s: String, checkExactNumeric: Boolean): TokenizedLiteral = {
    val decimal = BigDecimal(s)
    if (checkExactNumeric) {
      try {
        val longValue = decimal.toLongExact
        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          return newCachedLiteral(longValue.toInt, IntegerType)
        } else {
          return newCachedLiteral(longValue, LongType)
        }
      } catch {
        case _: ArithmeticException =>
      }
    }
    val precision = decimal.precision
    val scale = decimal.scale
    val sysDefaultType = DecimalType.SYSTEM_DEFAULT
    if (precision == sysDefaultType.precision && scale == sysDefaultType.scale) {
      newCachedLiteral(Decimal(decimal), sysDefaultType)
    } else {
      val userDefaultType = DecimalType.USER_DEFAULT
      if (precision == userDefaultType.precision && scale == userDefaultType.scale) {
        newCachedLiteral(Decimal(decimal), userDefaultType)
      } else {
        newCachedLiteral(Decimal(decimal), DecimalType(Math.max(precision, scale), scale))
      }
    }
  }

  private def toNumericLiteral(s: String): TokenizedLiteral = {
    // quick pass through the string to check for floats
    var noDecimalPoint = true
    var index = 0
    val len = s.length
    // use double if ending with D/d, float for F/f and long for L/l

    s.charAt(len - 1) match {
      case 'D' | 'd' =>
        if (s.length > 2) {
          s.charAt(len - 2) match {
            case 'B' | 'b' => return toDecimalLiteral(s.substring(0, len - 2),
              checkExactNumeric = false)
            case _ => return newCachedLiteral(
              java.lang.Double.parseDouble(s.substring(0, len - 1)), DoubleType)
          }
        } else {
          return newCachedLiteral(
            java.lang.Double.parseDouble(s.substring(0, len - 1)), DoubleType)
        }
      case 'F' | 'f' => return newCachedLiteral(
        java.lang.Float.parseFloat(s.substring(0, len - 1)), FloatType)
      case 'L' | 'l' => return newCachedLiteral(
        java.lang.Long.parseLong(s.substring(0, len - 1)), LongType)
      case 'S' | 's' => return newCachedLiteral(
        java.lang.Short.parseShort(s.substring(0, len - 1)), ShortType)
      case 'B' | 'b' | 'Y' | 'y' => return newCachedLiteral(
        java.lang.Byte.parseByte(s.substring(0, len - 1)), ByteType)
      case _ =>
    }
    while (index < len) {
      val c = s.charAt(index)
      if (noDecimalPoint && c == '.') {
        noDecimalPoint = false
      } else if (c == 'e' || c == 'E') {
        // follow the behavior in MS SQL Server
        // https://msdn.microsoft.com/en-us/library/ms179899.aspx
        return newCachedLiteral(java.lang.Double.parseDouble(s), DoubleType)
      }
      index += 1
    }
    if (noDecimalPoint) {
      // case of integral value
      // most cases should be handled by Long, so try that first
      try {
        val longValue = java.lang.Long.parseLong(s)
        if (longValue >= Int.MinValue && longValue <= Int.MaxValue) {
          newCachedLiteral(longValue.toInt, IntegerType)
        } else {
          newCachedLiteral(longValue, LongType)
        }
      } catch {
        case _: NumberFormatException =>
          toDecimalLiteral(s, checkExactNumeric = true)
      }
    } else {
      toDecimalLiteral(s, checkExactNumeric = false)
    }
  }

  private def updatePerTableQueryHint(tableIdent: TableIdentifier,
      optAlias: Option[(String, Seq[String])]): Unit = {
    if (queryHints.isEmpty) return
    val indexHint = queryHints.remove(QueryHint.Index.toString)
    if (indexHint ne null) {
      val table = optAlias match {
        case Some(a) => a._1
        case _ => tableIdent.unquotedString
      }
      queryHints.put(QueryHint.Index.toString + table, indexHint)
    }
  }

  private final def assertNoQueryHint(plan: LogicalPlan,
      optAlias: Option[(String, Seq[String])]): Unit = {
    if (!queryHints.isEmpty) {
      val hintStr = QueryHint.Index.toString
      queryHints.forEach(new BiConsumer[String, String] {
        override def accept(key: String, value: String): Unit = {
          if (key.startsWith(hintStr)) {
            val tableString = optAlias match {
              case Some(a) => a._1
              case None => plan.treeString(verbose = false)
            }
            throw new ParseException(
              s"Query hint '$hintStr' cannot be applied to derived table: $tableString")
          }
        }
      })
    }
  }

  private final def handleException[T](body: => T, valueType: String, s: String): T = {
    try {
      body
    } catch {
      case e: ParseException => throw e
      case e: Exception =>
        throw new ParseException(s"Exception parsing '$s' of type '$valueType'", Some(e))
    }
  }

  protected final def stringLiterals: Rule1[String] = rule {
    stringLiteral ~ (
        stringLiteral. + ~> ((s: String, ss: Seq[String]) => (s +: ss).mkString) |
        MATCH.asInstanceOf[Rule[String::HNil, String::HNil]]
    )
  }

  protected final def literal: Rule1[Expression] = rule {
    stringLiterals ~> ((s: String) => newCachedLiteral(UTF8String.fromString(s), StringType)) |
    numericLiteral ~> ((s: String) => handleException(toNumericLiteral(s), "number", s)) |
    booleanLiteral ~> ((b: Boolean) => newCachedLiteral(b, BooleanType)) |
    NULL ~> (() => newLiteral(null, NullType)) | // no ParamLiterals for nulls
    (ch('X') | ch('x')) ~ ws ~ stringLiteral ~> ((s: String) =>
      handleException(newCachedLiteral(DatatypeConverter.parseHexBinary(
        if ((s.length & 0x1) == 1) "0" + s else s), BinaryType), "hex", s)) |
    DATE ~ stringLiteral ~> ((s: String) =>
      handleException(newCachedLiteral(DateTimeUtils.fromJavaDate(
        java.sql.Date.valueOf(s)), DateType), "date", s)) |
    TIMESTAMP ~ stringLiteral ~> ((s: String) =>
      handleException(newCachedLiteral(DateTimeUtils.fromJavaTimestamp(
        java.sql.Timestamp.valueOf(s)), TimestampType), "timestamp", s)) |
    intervalExpression
  }

  protected final def questionMark: Rule1[Expression] = rule {
    '?' ~ ws ~> (() => {
      _questionMarkCounter += 1
      if (_isPreparePhase) {
        // MutableInt as value of ParamLiteral stores the position of ?
        val counter = new MutableInt
        counter.value = _questionMarkCounter
        counter.isNull = false
        // foldable condition same as that in newTokenizedLiteral
        ParamLiteral(counter, NullType, 0, execId = -1, tokenized = true).
            markFoldable(!cacheLiteral)
      } else {
        assert(_parameterValueSet.isDefined,
          "For Prepared Statement, Parameter constants are not provided")
        val (scalaTypeVal, dataType) = session.getParameterValue(
          _questionMarkCounter, _parameterValueSet.get, _preparedParamsTypesInfo)
        val catalystTypeVal = CatalystTypeConverters.createToCatalystConverter(
          dataType)(scalaTypeVal)
        newCachedLiteral(catalystTypeVal, dataType)
      }
    })
  }

  private[sql] final def addCachedLiteral(v: Any, dataType: DataType): TokenizedLiteral = {
    if (session.planCaching) addParamLiteralToContext(v, dataType)
    else new TokenLiteral(v, dataType)
  }

  protected final def newCachedLiteral(v: Any, dataType: DataType): TokenizedLiteral = {
    if (cacheLiteral) addCachedLiteral(v, dataType)
    else new TokenLiteral(v, dataType)
  }

  protected final def newLiteral(v: Any, dataType: DataType): TokenizedLiteral = {
    new TokenLiteral(v, dataType)
  }

  protected final def intervalType: Rule1[DataType] = rule {
    INTERVAL ~> (() => CalendarIntervalType)
  }

  private final def fromCalendarString(unit: String, s: String): CalendarInterval =
    handleException(CalendarInterval.fromSingleUnitString(unit, s), s"interval($unit)", s)

  protected final def intervalExpression: Rule1[Expression] = rule {
    INTERVAL ~ (
        stringLiteral ~ (
            YEAR ~ TO ~ MONTH ~> ((s: String) => handleException(
              CalendarInterval.fromYearMonthString(s), "interval(year to month)", s)) |
            DAY ~ TO ~ (SECOND | SECS) ~> ((s: String) => handleException(
              CalendarInterval.fromDayTimeString(s), "interval(day to second)", s))
        ) |
        (stringLiteral | integral | expression) ~ (
            YEAR ~> ((v: Any) => v match {
              case s: String => fromCalendarString("year", s)
              case _ => v.asInstanceOf[Expression] -> -1L
            }) |
            MONTH ~> ((v: Any) => v match {
              case s: String => fromCalendarString("month", s)
              case _ => v.asInstanceOf[Expression] -> -2L
            }) |
            WEEK ~> ((v: Any) => v match {
              case s: String => fromCalendarString("week", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_WEEK
            }) |
            DAY ~> ((v: Any) => v match {
              case s: String => fromCalendarString("day", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_DAY
            }) |
            HOUR ~> ((v: Any) => v match {
              case s: String => fromCalendarString("hour", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_HOUR
            }) |
            (MINUTE | MINS) ~> ((v: Any) => v match {
              case s: String => fromCalendarString("minute", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_MINUTE
            }) |
            (SECOND | SECS) ~> ((v: Any) => v match {
              case s: String => fromCalendarString("second", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_SECOND
            }) |
            (MILLISECOND | MILLIS) ~> ((v: Any) => v match {
              case s: String => fromCalendarString("millisecond", s)
              case _ => v.asInstanceOf[Expression] -> CalendarInterval.MICROS_PER_MILLI
            }) |
            (MICROSECOND | MICROS) ~> ((v: Any) => v match {
              case s: String => fromCalendarString("microsecond", s)
              case _ => v.asInstanceOf[Expression] -> 1L
            })
        )
    ). + ~> ((s: Seq[Any]) =>
      if (s.length == 1) s.head match {
        case c: CalendarInterval => newCachedLiteral(c, CalendarIntervalType)
        case (e: Expression, u: Long) => IntervalExpression(e :: Nil, u :: Nil)
      } else if (s.forall(_.isInstanceOf[CalendarInterval])) {
        newCachedLiteral(s.reduce((v1, v2) => v1.asInstanceOf[CalendarInterval].add(
          v2.asInstanceOf[CalendarInterval])), CalendarIntervalType)
      } else {
        val (expressions, units) = s.flatMap {
          case c: CalendarInterval =>
            if (c.months != 0) {
              newLiteral(c.months.toLong, LongType) -> -2L ::
                  newLiteral(c.microseconds, LongType) -> 1L :: Nil
            } else {
              newLiteral(c.microseconds, LongType) -> 1L :: Nil
            }
          case p => p.asInstanceOf[(Expression, Long)] :: Nil
        }.unzip
        IntervalExpression(expressions, units)
      })
  }

  protected final def unsignedFloat: Rule1[String] = rule {
    capture(
      CharPredicate.Digit.* ~ '.' ~ CharPredicate.Digit. + ~
          scientificNotation.? |
      CharPredicate.Digit. + ~ scientificNotation
    ) ~ ws
  }

  final def namedExpression: Rule1[Expression] = rule {
    expression ~ (AS ~ (identifierExt2 | identifierList) | (strictIdentifier | identifierList)).? ~>
        ((e: Expression, a: Any) => a match {
          case None => e
          case Some(name) if name.isInstanceOf[String] => Alias(e, name.asInstanceOf[String])()
          case Some(names) => MultiAlias(e, names.asInstanceOf[Seq[String]])
        })
  }

  final def namedExpressionSeq: Rule1[Seq[Expression]] = rule {
    namedExpression + commaSep
  }

  final def parseDataType: Rule1[DataType] = rule {
    ws ~ dataType ~ EOI
  }

  final def parseExpression: Rule1[Expression] = rule {
    ws ~ namedExpression ~ EOI
  }

  final def parseTableIdentifier: Rule1[TableIdentifier] = rule {
    ws ~ tableIdentifier ~ EOI
  }

  final def parseIdentifier: Rule1[String] = rule {
    ws ~ identifier ~ EOI
  }

  final def parseIdentifiers: Rule1[Seq[String]] = rule {
    ws ~ (identifier + commaSep) ~ EOI
  }

  final def parseFunctionIdentifier: Rule1[FunctionIdentifier] = rule {
    ws ~ functionIdentifier ~ EOI
  }

  final def parseTableSchema: Rule1[Seq[StructField]] = rule {
    ws ~ (column + commaSep) ~ EOI
  }

  protected final def handleExtract(fn: String, e: Expression): Expression = {
    Utils.toLowerCase(fn) match {
      case "year" => Year(e)
      case "quarter" => Quarter(e)
      case "month" => Month(e)
      case "week" => WeekOfYear(e)
      case "day" => DayOfMonth(e)
      case "dayofweek" => DayOfWeek(e)
      case "hour" => Hour(e)
      case "minute" => Minute(e)
      case "second" => Second(e)
      case _ => throw new ParseException(s"Literals of type '$fn' are currently not supported.")
    }
  }

  protected final def foldableFunctionsExpressionHandler(exprs: IndexedSeq[Expression],
      fnName: String): Seq[Expression] = Consts.FOLDABLE_FUNCTIONS.get(fnName) match {
    case null => exprs
    case args if args.length == 0 =>
      // disable plan caching for these functions
      session.planCaching = false
      exprs
    case args =>
      exprs.indices.map(index => exprs(index).transformUp {
        case l: TokenizedLiteral if (args(0) == -3 && !Ints.contains(args, index)) ||
            (args(0) != -3 && (Ints.contains(args, index) ||
                // all args          // all odd args
                (args(0) == -10) || (args(0) == -1 && (index & 0x1) == 1) ||
                // all even args
                (args(0) == -2 && (index & 0x1) == 0))) =>
          l match {
            case pl: ParamLiteral if pl.tokenized && _isPreparePhase =>
              throw new ParseException(s"function $fnName cannot have " +
                  s"parameterized argument at position ${index + 1}")
            case _ =>
          }
          removeIfParamLiteralFromContext(l)
          newLiteral(l.value, l.dataType)
        case e => e
      })
  }

  protected final def primaryExpression: Rule1[Expression] = rule {
    literal |
    questionMark |
    '*' ~ ws ~> (() => UnresolvedStar(None)) |
    CAST ~ O_PAREN ~ expression ~ AS ~ (dataType | intervalType) ~ C_PAREN ~> (Cast(_, _)) |
    CASE ~ (
        whenClause. + ~ (ELSE ~ expression).? ~ END ~> ((altPart: Seq[(Expression, Expression)],
            elsePart: Any) => CaseWhen(altPart, elsePart.asInstanceOf[Option[Expression]])) |
        expression ~ whenClause. + ~ (ELSE ~ expression).? ~ END ~>
            ((key: Expression, altPart: Seq[(Expression, Expression)], elsePart: Any) =>
              CaseWhen(altPart.map(e => EqualTo(key, e._1) -> e._2),
                elsePart.asInstanceOf[Option[Expression]]))
    ) |
    CURRENT_DATE ~ (O_PAREN ~ C_PAREN).? ~> (() => CurrentDate()) |
    CURRENT_TIMESTAMP ~ (O_PAREN ~ C_PAREN).? ~> CurrentTimestamp |
    STRUCT ~ O_PAREN ~ namedExpressionSeq.? ~ C_PAREN ~> ((e: Any) => e match {
      case None => CreateStruct(Nil)
      case Some(s) => CreateStruct(s.asInstanceOf[Seq[Expression]])
    }) |
    FIRST ~ O_PAREN ~ expression ~ (
        IGNORE ~ NULLS ~> ((e: Expression) =>
          First(e, newLiteral(true, BooleanType)).toAggregateExpression()) |
        (commaSep ~ expression).* ~> ((e: Expression, args: Any) => UnresolvedFunction(
          Consts.FIRST.lower, foldableFunctionsExpressionHandler(e +: args.asInstanceOf[
              Seq[Expression]].toIndexedSeq, Consts.FIRST.lower), isDistinct = false))
    ) ~ C_PAREN |
    LAST ~ O_PAREN ~ expression ~ (
        IGNORE ~ NULLS ~> ((e: Expression) =>
          Last(e, newLiteral(true, BooleanType)).toAggregateExpression()) |
        (commaSep ~ expression).* ~> ((e: Expression, args: Any) => UnresolvedFunction(
          Consts.LAST.lower, foldableFunctionsExpressionHandler(e +: args.asInstanceOf[
              Seq[Expression]].toIndexedSeq, Consts.LAST.lower), isDistinct = false))
    ) ~ C_PAREN |
    POSITION ~ O_PAREN ~ expression ~ (
        IN ~ expression ~> ((sub: Expression, str: Expression) => new StringLocate(sub, str)) |
        (commaSep ~ expression). + ~> ((sub: Expression, args: Any) => UnresolvedFunction(
          Consts.POSITION.lower, sub +: args.asInstanceOf[Seq[Expression]], isDistinct = false))
    ) ~ C_PAREN |
    TRIM ~ O_PAREN ~ (
        (BOTH ~ push(0) | LEADING ~ push(1) | TRAILING ~ push(2)) ~ valueExpression ~
            FROM ~ valueExpression ~> { (flag: Int, trim: Expression, src: Expression) =>
          val funcName = flag match {
            case 0 => Consts.TRIM.lower
            case 1 => "ltrim"
            case 2 => "rtrim"
          }
          UnresolvedFunction(funcName, trim :: src :: Nil, isDistinct = false)
        } |
        (expression + commaSep) ~> ((args: Seq[Expression]) =>
          UnresolvedFunction(Consts.TRIM.lower, args, isDistinct = false))
    ) ~ C_PAREN |
    EXTRACT ~ O_PAREN ~ identifier ~ FROM ~ valueExpression ~ C_PAREN ~> handleExtract _ |
    identifierWithQuotedFlag ~ (
      "->" ~ ws ~ expression ~> ((id: String, e: Expression) =>
        internals.newLambdaFunction(e, UnresolvedAttribute(id :: Nil) :: Nil)) |
      stringLiteral ~> ((v: String, valueType: String) =>
        try {
          Utils.toLowerCase(valueType) match {
            case "date" => newCachedLiteral(DateTimeUtils.fromJavaDate(
              java.sql.Date.valueOf(v)), DateType)
            case "timestamp" => newCachedLiteral(DateTimeUtils.fromJavaTimestamp(
              java.sql.Timestamp.valueOf(v)), TimestampType)
            case "x" => newCachedLiteral(DatatypeConverter.parseHexBinary(
              if ((v.length & 0x1) == 1) "0" + v else v), BinaryType)
            case _ =>
              throw new ParseException(s"Literals of type '$valueType' are not supported.")
          }
        } catch {
          case e: IllegalArgumentException =>
            throw new ParseException(s"Exception parsing '$v' of type '$valueType'", Some(e))
        }) |
      ('.' ~ ws ~ identifierWithQuotedFlag).* ~> { (id: String, ids: Any) =>
        val idSeq = ids.asInstanceOf[Seq[String]]
        if (idSeq.isEmpty) id :: Nil else (id +: idSeq).toIndexedSeq
      } ~ (
        '.' ~ ws ~ '*' ~ ws ~> ((ids: Seq[String]) => UnresolvedStar(Some(ids))) |
        O_PAREN ~ setQuantifier ~ (expression * commaSep) ~ C_PAREN ~ (OVER ~ windowSpec).? ~> {
          (ids: Seq[String], a: Option[Boolean], args: Any, w: Any) =>
            val id = ids.head
            val fnName = ids.length match {
              case 1 => new FunctionIdentifier(id)
              case 2 => new FunctionIdentifier(ids(1), Some(id))
              case _ =>
                throw new ParseException(s"Unsupported function name '${ids.mkString(".")}'")
            }
            val allExprs = args.asInstanceOf[Seq[Expression]].toIndexedSeq
            val exprs = foldableFunctionsExpressionHandler(allExprs, id)
            val function = if (!a.contains(false)) {
              // convert COUNT(*) to COUNT(1)
              if (fnName.database.isEmpty && fnName.funcName.equalsIgnoreCase("count") &&
                  exprs.length == 1 && exprs.head == UnresolvedStar(None)) {
                UnresolvedFunction(fnName, newLiteral(1, IntegerType) :: Nil, isDistinct = false)
              } else UnresolvedFunction(fnName, exprs, isDistinct = false)
            } else {
              UnresolvedFunction(fnName, exprs, isDistinct = true)
            }
            w.asInstanceOf[Option[WindowSpec]] match {
              case None => function
              case Some(spec: WindowSpecDefinition) => WindowExpression(function, spec)
              case Some(ref: WindowSpecReference) => UnresolvedWindowExpression(function, ref)
            }
        } |
        // noinspection ScalaUnnecessaryParentheses
        MATCH ~> { (ids: Seq[String]) =>
          val idLen = ids.length
          if (idLen <= 2 && quotedRegexColumnNames && isQuoted && _inProjection) {
            if (idLen == 1) internals.newUnresolvedRegex(ids.head, None, caseSensitive)
            else internals.newUnresolvedRegex(ids(1), Some(ids.head), caseSensitive)
          } else UnresolvedAttribute(ids)
        }
      )
    ) |
    O_PAREN ~ (
        (namedExpression + commaSep) ~ C_PAREN ~ (
            "->" ~ ws ~ expression ~> ((args: Seq[Expression], e: Expression) =>
              internals.newLambdaFunction(e, args)) |
            MATCH ~> ((exprs: Seq[Expression]) =>
              if (exprs.length == 1) exprs.head else CreateStruct(exprs))
        ) |
        // noinspection ScalaUnnecessaryParentheses
        query ~ C_PAREN ~> { (plan: LogicalPlan) =>
          session.planCaching = false // never cache scalar subquery plans
          ScalarSubquery(plan)
        }
    ) |
    '{' ~ ws ~ FN ~ functionIdentifier ~ expressionList ~
        '}' ~ ws ~> { (fn: FunctionIdentifier, e: Seq[Expression]) =>
      val exprs = foldableFunctionsExpressionHandler(e.toIndexedSeq, fn.funcName)
      fn match {
        case f if f.funcName.equalsIgnoreCase("timestampadd") =>
          assert(exprs.length == 3)
          assert(exprs.head.isInstanceOf[UnresolvedAttribute] &&
              exprs.head.asInstanceOf[UnresolvedAttribute].name.equalsIgnoreCase("sql_tsi_day"))
          DateAdd(exprs(2), exprs(1))
        case f => UnresolvedFunction(f, exprs, isDistinct = false)
      }
    }
  }

  /*
   * Various '*Expression' rules below are arranged as per operator precedence:
   * [], .                               => resolveExpression
   * UNARY (+, -, ~)                     => unaryExpression
   * *, /, DIV, %                        => multPrecExpression
   * +, -, ||, &, |, ^                   => sumPrecExpression
   * comparison operators, >>, >>>, <<   => valueExpression (same as in SqlBase.g4)
   * NOT, EXISTS, IS NOT? NULL, LIKE etc => booleanExpression
   * AND                                 => andExpression
   * OR                                  => expression
   *
   * Lower precedence rules refer to the rule one step higher.
   */

  private final def resolveExpression: Rule1[Expression] = rule {
    primaryExpression ~ (
        '[' ~ ws ~ valueExpression ~ ']' ~ ws ~> UnresolvedExtractValue |
        '.' ~ ws ~ identifier ~> ((base: Expression, attr: String) =>
          UnresolvedExtractValue(base, newLiteral(UTF8String.fromString(attr), StringType)))
    ).*
  }

  private final def unaryExpression: Rule1[Expression] = rule {
    resolveExpression |
    capture(Consts.unaryOperator) ~ ws ~ resolveExpression ~> ((s: String, e: Expression) =>
      if (s.charAt(0) == '-') UnaryMinus(e) else if (s.charAt(0) == '~') BitwiseNot(e) else e)
  }

  private final def multPrecExpression: Rule1[Expression] = rule {
    unaryExpression ~ (
        capture(Consts.multPrecOperator) ~ ws ~ unaryExpression ~>
            ((e1: Expression, op: String, e2: Expression) => op.charAt(0) match {
              case '*' => Multiply(e1, e2)
              case '/' => Divide(e1, e2)
              case '%' => Remainder(e1, e2)
              case c => throw new IllegalStateException(s"Unexpected operator '$c'")
            }) |
        DIV ~ unaryExpression ~> ((e1: Expression, e2: Expression) =>
          Cast(Divide(e1, e2), LongType))
    ).*
  }

  private final def sumPrecExpression: Rule1[Expression] = rule {
    multPrecExpression ~ (
        capture(Consts.sumPrecOperator) ~ ws ~ multPrecExpression ~>
            ((e1: Expression, op: String, e2: Expression) => op.charAt(0) match {
              case '+' => Add(e1, e2)
              case '-' => Subtract(e1, e2)
              case '&' => BitwiseAnd(e1, e2)
              case '^' => BitwiseXor(e1, e2)
              case c => throw new IllegalStateException(s"Unexpected operator '$c'")
            }) |
        '|' ~ (
            '|' ~ ws ~ multPrecExpression ~> ((e1: Expression, e2: Expression) =>
              e1 match {
                case Concat(children) => Concat(children :+ e2)
                case _ => Concat(e1 :: e2 :: Nil)
              }) |
            ws ~ multPrecExpression ~> BitwiseOr
        )
    ).*
  }

  protected final def valueExpression: Rule1[Expression] = rule {
    sumPrecExpression ~ (
        '=' ~ '='.? ~ ws ~ sumPrecExpression ~> EqualTo |
        '>' ~ (
            '=' ~ ws ~ sumPrecExpression ~> GreaterThanOrEqual |
            '>' ~ (
                '>' ~ ws ~ sumPrecExpression ~> ShiftRightUnsigned |
                ws ~ sumPrecExpression ~> ShiftRight
            ) |
            ws ~ sumPrecExpression ~> GreaterThan
        ) |
        '<' ~ (
            '=' ~ (
                '>' ~ ws ~ sumPrecExpression ~> EqualNullSafe |
                ws ~ sumPrecExpression ~> LessThanOrEqual
            ) |
            '>' ~ ws ~ sumPrecExpression ~>
                ((e1: Expression, e2: Expression) => Not(EqualTo(e1, e2))) |
            '<' ~ ws ~ sumPrecExpression ~> ShiftLeft |
            ws ~ sumPrecExpression ~> LessThan
        ) |
        '!' ~ (
            '=' ~ ws ~ sumPrecExpression ~> ((e1: Expression, e2: Expression) =>
              Not(EqualTo(e1, e2))) |
            '<' ~ ws ~ sumPrecExpression ~> GreaterThanOrEqual |
            '>' ~ ws ~ sumPrecExpression ~> LessThanOrEqual
        )
    ).*
  }

  protected final def toLikeExpression(left: Expression, right: TokenizedLiteral): Expression = {
    val pattern = right.valueString
    removeIfParamLiteralFromContext(right)
    if (Consts.optimizableLikePattern.matcher(pattern).matches()) {
      val size = pattern.length
      val expression = if (pattern.charAt(0) == '%') {
        if (pattern.charAt(size - 1) == '%') {
          Contains(left, addCachedLiteral(
            UTF8String.fromString(pattern.substring(1, size - 1)), StringType))
        } else {
          EndsWith(left, addCachedLiteral(
            UTF8String.fromString(pattern.substring(1)), StringType))
        }
      } else if (pattern.charAt(size - 1) == '%') {
        StartsWith(left, addCachedLiteral(
          UTF8String.fromString(pattern.substring(0, size - 1)), StringType))
      } else {
        // check for startsWith and endsWith
        val wildcardIndex = pattern.indexOf('%')
        if (wildcardIndex != -1) {
          val prefix = pattern.substring(0, wildcardIndex)
          val postfix = pattern.substring(wildcardIndex + 1)
          val prefixLiteral = addCachedLiteral(UTF8String.fromString(prefix), StringType)
          val suffixLiteral = addCachedLiteral(UTF8String.fromString(postfix), StringType)
          And(GreaterThanOrEqual(Length(left),
            addCachedLiteral(prefix.length + postfix.length, IntegerType)),
            And(StartsWith(left, prefixLiteral), EndsWith(left, suffixLiteral)))
        } else {
          // no wildcards
          EqualTo(left, addCachedLiteral(UTF8String.fromString(pattern), StringType))
        }
      }
      expression
    } else {
      LikeEscapeSimplification.simplifyLike(this,
        Like(left, newLiteral(right.value, right.dataType)), left, pattern)
    }
  }

  /**
   * Expressions which can be preceded by a NOT. This assumes one expression
   * already pushed on stack which it will pop and then push back the result
   * Expression (hence the slightly odd looking type)
   */
  private final def predicate: Rule[Expression :: HNil,
      Expression :: HNil] = rule {
    LIKE ~ valueExpression ~>
        ((e1: Expression, e2: Expression) => e2 match {
          case l: TokenizedLiteral if !l.value.isInstanceOf[MutableInt] => toLikeExpression(e1, l)
          case _ => Like(e1, e2)
        }) |
    IN ~ O_PAREN ~ (
        // query should be before expression else '(' query ')' gets resolved as a scalar subquery
        query ~> ((e1: Expression, plan: LogicalPlan) => internals.newInSubquery(e1, plan)) |
        (expression + commaSep) ~> ((e: Expression, es: Any) =>
          In(e, es.asInstanceOf[Seq[Expression]]))
    ) ~ C_PAREN |
    BETWEEN ~ valueExpression ~ AND ~ valueExpression ~>
        ((e: Expression, el: Expression, eu: Expression) =>
          And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))) |
    (RLIKE | REGEXP) ~ valueExpression ~>
        ((e1: Expression, e2: Expression) => e2 match {
          case l: TokenizedLiteral if !l.value.isInstanceOf[MutableInt] =>
            removeIfParamLiteralFromContext(l)
            RLike(e1, newLiteral(l.value, l.dataType))
          case _ => RLike(e1, e2)
        })
  }

  private final def booleanExpression: Rule1[Expression] = rule {
    NOT ~ booleanExpression ~> Not |
    // exists is also a function
    EXISTS ~ O_PAREN ~ (
        query ~> (Exists(_)) |
        (expression + commaSep) ~> ((args: Seq[Expression]) =>
          UnresolvedFunction(Consts.EXISTS.lower, args, isDistinct = false))
    ) ~ C_PAREN |
    valueExpression ~ (
        predicate |
        NOT ~ predicate ~> Not |
        IS ~ (
            (NOT ~ push(true)).? ~ NULL ~> ((e: Expression, not: Any) =>
              if (not == None) IsNull(e) else IsNotNull(e)) |
            (NOT ~ push(true)).? ~ DISTINCT ~ FROM ~
                valueExpression ~> ((e1: Expression, not: Any, e2: Expression) =>
              if (not == None) Not(EqualNullSafe(e1, e2)) else EqualNullSafe(e1, e2))
        ) |
        MATCH.asInstanceOf[Rule[Expression::HNil, Expression::HNil]]
    )
  }

  private final def andExpression: Rule1[Expression] = rule {
    booleanExpression ~ (AND ~ booleanExpression ~> And).*
  }

  protected final def expression: Rule1[Expression] = rule {
    andExpression ~ (OR ~ andExpression ~> Or).*
  }

  protected final def expressionList: Rule1[Seq[Expression]] = rule {
    O_PAREN ~ (expression * commaSep) ~ C_PAREN ~> ((e: Any) => e.asInstanceOf[Seq[Expression]])
  }

  protected final def streamWindowOptions: Rule1[(Duration,
      Option[Duration])] = rule {
    WINDOW ~ O_PAREN ~ DURATION ~ durationUnit ~ (commaSep ~
        SLIDE ~ durationUnit).? ~ C_PAREN ~> ((d: Duration, s: Any) =>
      (d, s.asInstanceOf[Option[Duration]]))
  }

  protected final def extractGroupingSet(
      child: LogicalPlan,
      aggregations: Seq[NamedExpression],
      groupByExprs: Seq[Expression],
      groupingSets: Seq[Seq[Expression]]): LogicalPlan = {
    internals.newGroupingSet(groupingSets, groupByExprs, child, aggregations)
  }

  protected final def groupingSetExpr: Rule1[Seq[Expression]] = rule {
    expressionList |
    (expression + commaSep)
  }

  protected final def cubeRollUpGroupingSet: Rule1[
      (Seq[Seq[Expression]], String)] = rule {
    WITH ~ (
        CUBE ~> (() => (Seq(Seq[Expression]()), "CUBE")) |
        ROLLUP ~> (() => (Seq(Seq[Expression]()), "ROLLUP"))
    ) |
    GROUPING ~ SETS ~ (O_PAREN ~ (groupingSetExpr + commaSep) ~ C_PAREN)  ~>
        ((gs: Seq[Seq[Expression]]) => (gs, "GROUPINGSETS"))
  }

  protected final def groupBy: Rule1[(Seq[Expression],
      Seq[Seq[Expression]], String)] = rule {
    GROUP ~ BY ~ (expression + commaSep) ~ cubeRollUpGroupingSet.? ~>
        ((g: Any, crgs: Any) => {
          // change top-level tokenized literals to literals for GROUP BY 1 kind of queries
          val groupingExprs = g.asInstanceOf[Seq[Expression]].map {
            case p: ParamLiteral => removeParamLiteralFromContext(p); p.asLiteral
            case l: TokenLiteral => l
            case e => e
          }
          val cubeRollupGrSetExprs = crgs.asInstanceOf[Option[(Seq[
              Seq[Expression]], String)]] match {
            case None => (Seq(Nil), "")
            case Some(e) => e
          }
          (groupingExprs, cubeRollupGrSetExprs._1, cubeRollupGrSetExprs._2)
        })
  }

  private def createSample(fraction: Double): LogicalPlan => Sample = child => {
    // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
    // function takes X PERCENT as the input and the range of X is [0, 100], we need to
    // adjust the fraction.
    val eps = RandomSampler.roundingEpsilon
    if (!(fraction >= 0.0 - eps && fraction <= 1.0 + eps)) {
      throw new ParseException(s"Sampling fraction ($fraction) must be on interval [0, 1]")
    }
    internals.newTableSample(0.0, fraction, withReplacement = false,
      (math.random * 1000).toInt, child)
  }

  protected final def toDouble(s: String): Double = {
    handleException(toNumericLiteral(s), "double", s).eval(EmptyRow) match {
      case n: Number => n.doubleValue()
      case d: Decimal => d.toDouble
      case o => throw new ParseException(s"Cannot convert '$o' to double")
    }
  }

  protected final def sample: Rule1[LogicalPlan => LogicalPlan] = rule {
    TABLESAMPLE ~ O_PAREN ~ (
        numericLiteral ~ PERCENT ~> ((s: String) => createSample(toDouble(s))) |
        integral ~ OUT ~ OF ~ integral ~> ((n: String, d: String) =>
          createSample(toDouble(n) / toDouble(d))) |
        expression ~ ROWS ~> ((e: Expression) => { child: LogicalPlan => Limit(e, child) })
    ) ~ C_PAREN
  }

  protected final def tableAlias: Rule1[(String, Seq[String])] = rule {
    (AS ~ identifierExt2 | strictIdentifier) ~ identifierList.? ~>
        ((alias: String, columnAliases: Any) => columnAliases match {
          case None => (alias, Nil)
          case Some(aliases) => (alias, aliases.asInstanceOf[Seq[String]])
        })
  }

  /**
   * Table name optionally followed by column names in parentheses that can appear in
   * queries or INSERT/PUT. In queries when column names are present, then it is interpreted
   * as a SQL function that takes those arguments and resolves to a set of rows
   * e.g. RANGE(...) function. In DML operations it represents a subset of columns of the
   * table on which operation will be performed with remaining columns of the table
   * using null/default values.
   */
  protected final def baseTable: Rule1[LogicalPlan] = rule {
    tableIdentifier ~ (
        expressionList ~> ((ident: TableIdentifier, e: Seq[Expression]) =>
          internals.newUnresolvedTableValuedFunction(ident.unquotedString, e, Nil)) |
        // noinspection ScalaUnnecessaryParentheses
        MATCH ~> ((ident: TableIdentifier) => UnresolvedRelation(ident))
    )
  }

  protected final def inlineTable: Rule1[LogicalPlan] = rule {
    VALUES ~ CACHE_LITERAL_SKIP ~ (expression + commaSep) ~ tableAlias.? ~
        CACHE_LITERAL_RESTORE ~> ((valuesExpr: Seq[Expression], a: Any) => {
      val rows = valuesExpr.map {
        // e.g. values (1), (2), (3)
        case struct: CreateNamedStruct => struct.valExprs
        // e.g. values 1, 2, 3
        case child => Seq(child)
      }
      val optAlias = a.asInstanceOf[Option[(String, Seq[String])]]
      val aliases = optAlias match {
        case Some(ids) if ids._2.nonEmpty => ids._2
        case _ => Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
      }
      optAlias match {
        case None => UnresolvedInlineTable(aliases, rows)
        case Some(id) => internals.newSubqueryAlias(id._1, UnresolvedInlineTable(aliases, rows))
      }
    })
  }

  protected final def handleSubqueryAlias(aliasSpec: Option[(String, Seq[String])],
      plan: LogicalPlan): LogicalPlan = aliasSpec match {
    case None =>
      // For un-aliased subqueries, use a default alias name that is not likely to conflict with
      // normal subquery names, so that parent operators can only access the columns in subquery by
      // unqualified names. Users can still use this special qualifier to access columns if they
      // know it, but that's not recommended.
      internals.newSubqueryAlias("__auto_generated_subquery_name", plan)
    case Some((alias, columnAliases)) =>
      internals.newUnresolvedColumnAliases(columnAliases, internals.newSubqueryAlias(alias, plan))
  }

  protected final def handleRelationAlias(aliasSpec: Option[(String, Seq[String])],
      u: UnresolvedRelation, plan: LogicalPlan): LogicalPlan = aliasSpec match {
    case None => plan
    case Some((alias, columnAliases)) =>
      val tableIdent = u.tableIdentifier
      updatePerTableQueryHint(tableIdent, aliasSpec)
      internals.newUnresolvedColumnAliases(columnAliases, internals.newSubqueryAlias(alias, plan))
  }

  private final def withWindow(window: Option[(Duration, Option[Duration])],
      plan: LogicalPlan): LogicalPlan = window match {
    case None => plan
    case Some(w) => WindowLogicalPlan(w._1, w._2, plan)
  }

  private final def withSample(sample: Option[LogicalPlan => LogicalPlan],
      plan: LogicalPlan): LogicalPlan = sample match {
    case None => plan
    case Some(s) => s(plan)
  }

  protected final def relationPrimary: Rule1[LogicalPlan] = rule {
    inlineTable |
    (baseTable | (O_PAREN ~ relation ~ C_PAREN) | queryNoWith) ~ streamWindowOptions.? ~
        sample.? ~ tableAlias.? ~> { (rel: LogicalPlan, w: Any, s: Any, a: Any) =>
      val window = w.asInstanceOf[Option[(Duration, Option[Duration])]]
      val sample = s.asInstanceOf[Option[LogicalPlan => LogicalPlan]]
      val optAlias = a.asInstanceOf[Option[(String, Seq[String])]]
      rel match {
        case u: UnresolvedRelation =>
          handleRelationAlias(optAlias, u, withSample(sample, withWindow(window, u)))
        case u: UnresolvedTableValuedFunction =>
          assertNoQueryHint(u, optAlias)
          if (optAlias.isEmpty) withSample(sample, withWindow(window, u))
          else {
            internals.newSubqueryAlias(optAlias.get._1,
              withSample(sample, withWindow(window, internals.newUnresolvedTableValuedFunction(
                u.functionName, u.functionArgs, optAlias.get._2))))
          }
        case _ =>
          assertNoQueryHint(rel, optAlias)
          handleSubqueryAlias(optAlias, withSample(sample, withWindow(window, rel)))
      }
    }
  }

  protected final def joinType: Rule1[JoinType] = rule {
    INNER ~> (() => Inner) |
    LEFT ~ (
        SEMI ~> (() => LeftSemi) |
        ANTI ~> (() => LeftAnti) |
        OUTER.? ~> (() => LeftOuter)
    ) |
    RIGHT ~ OUTER.? ~> (() => RightOuter) |
    FULL ~ OUTER.? ~> (() => FullOuter) |
    ANTI ~> (() => LeftAnti) |
    CROSS ~> (() => Cross)
  }

  protected final def ordering: Rule1[Seq[SortOrder]] = rule {
    ((expression ~ sortDirection.? ~ (NULLS ~ (FIRST ~ push(true) | LAST ~ push(false))).? ~>
        ((e: Expression, d: Any, n: Any) => (e, d, n))) + commaSep) ~> ((exprs: Any) =>
      exprs.asInstanceOf[Seq[(Expression, Option[SortDirection], Option[Boolean])]].map {
        case (c, d, n) =>
          // change top-level tokenized literals to literals for ORDER BY 1 kind of queries
          val child = c match {
            case p: ParamLiteral => removeParamLiteralFromContext(p); p.asLiteral
            case l: TokenLiteral => l
            case _ => c
          }
          val direction = d match {
            case Some(v) => v
            case None => Ascending
          }
          val nulls = n match {
            case Some(false) => NullsLast
            case Some(true) => NullsFirst
            case None => direction.defaultNullOrdering
          }
          internals.newSortOrder(child, direction, nulls)
      })
  }

  protected def queryOrganization: Rule1[LogicalPlan => LogicalPlan] = rule {
    (ORDER ~ BY ~ ordering ~> ((o: Seq[SortOrder]) =>
      (l: LogicalPlan) => Sort(o, global = true, l)) |
    SORT ~ BY ~ ordering ~ distributeBy.? ~> ((o: Seq[SortOrder], d: Any) =>
      (l: LogicalPlan) => Sort(o, global = false, d.asInstanceOf[Option[
          LogicalPlan => LogicalPlan]].map(_ (l)).getOrElse(l))) |
    distributeBy |
    CLUSTER ~ BY ~ (expression + commaSep) ~> ((e: Seq[Expression]) =>
      (l: LogicalPlan) => Sort(e.map(SortOrder(_, Ascending)), global = false,
        internals.newRepartitionByExpression(e,
          session.sessionState.conf.numShufflePartitions, l)))).? ~
    (WINDOW ~ ((identifier ~ AS ~ windowSpec ~>
        ((id: String, w: WindowSpec) => id -> w)) + commaSep)).? ~
    ((LIMIT ~ (capture(ALL) | CACHE_LITERAL_SKIP ~ expression ~ CACHE_LITERAL_RESTORE)) |
    fetchExpression).? ~> { (o: Any, w: Any, e: Any) => (l: LogicalPlan) =>
      val withOrder = o.asInstanceOf[Option[LogicalPlan => LogicalPlan]] match {
        case None => l
        case Some(m) => m(l)
      }
      val withWindow = w.asInstanceOf[Option[Seq[(String, WindowSpec)]]] match {
        case None => withOrder
        case Some(ws) =>
          val baseWindowMap = ws.toMap
          val windowMapView = baseWindowMap.mapValues {
            case WindowSpecReference(name) =>
              baseWindowMap.get(name) match {
                case Some(spec: WindowSpecDefinition) => spec
                case Some(_) => throw new ParseException(
                  s"Window reference '$name' is not a window specification")
                case None => throw new ParseException(
                  s"Cannot resolve window reference '$name'")
              }
            case spec: WindowSpecDefinition => spec
          }

          // Note that mapValues creates a view, so force materialization.
          WithWindowDefinition(windowMapView.map(identity), withOrder)
      }
      // can be None or Some[Expression] (LIMIT or FETCH FIRST) or Some[String] (LIMIT ALL)
      e match {
        case Some(e: Expression) => Limit(e, withWindow)
        case _ => withWindow
      }
    }
  }

  protected final def fetchExpression: Rule1[Expression] = rule {
    FETCH ~ FIRST ~ CACHE_LITERAL_SKIP ~ integral.? ~ CACHE_LITERAL_RESTORE ~
        ((ROW | ROWS) ~ ONLY) ~> ((f: Any) => {
      f.asInstanceOf[Option[String]] match {
        case None => newLiteral(1, IntegerType)
        case Some(s) => newLiteral(s.toInt, IntegerType)
      }
    })
  }

  protected final def distributeBy: Rule1[LogicalPlan => LogicalPlan] = rule {
    DISTRIBUTE ~ BY ~ (expression + commaSep) ~> ((e: Seq[Expression]) =>
      (l: LogicalPlan) => internals.newRepartitionByExpression(
        e, session.sessionState.conf.numShufflePartitions, l))
  }

  protected final def windowSpec: Rule1[WindowSpec] = rule {
    O_PAREN ~ ((PARTITION | DISTRIBUTE | CLUSTER) ~ BY ~ (expression +
        commaSep)).? ~ ((ORDER | SORT) ~ BY ~ ordering).? ~ windowFrame.? ~
        C_PAREN ~> ((p: Any, o: Any, w: Any) =>
      WindowSpecDefinition(
        p.asInstanceOf[Option[Seq[Expression]]].getOrElse(Nil),
        o.asInstanceOf[Option[Seq[SortOrder]]].getOrElse(Nil),
        w.asInstanceOf[Option[SpecifiedWindowFrame]].getOrElse(UnspecifiedFrame))) |
    identifier ~> WindowSpecReference
  }

  protected final def windowFrame: Rule1[SpecifiedWindowFrame] = rule {
    (RANGE ~> (() => RangeFrame) | ROWS ~> (() => RowFrame)) ~ (
        BETWEEN ~ frameBound ~ AND ~ frameBound ~> ((t: FrameType,
            s: Any, e: Any) => internals.newSpecifiedWindowFrame(t, s, e)) |
    frameBound ~> ((t: FrameType, s: Any) =>
      internals.newSpecifiedWindowFrame(t, s, CurrentRow))
    )
  }

  protected final def frameBound: Rule1[Any] = rule {
    UNBOUNDED ~ (
        PRECEDING ~> (() => internals.newFrameBoundary(FrameBoundaryType.UnboundedPreceding)) |
        FOLLOWING ~> (() => internals.newFrameBoundary(FrameBoundaryType.UnboundedFollowing))
    ) |
    CURRENT ~ ROW ~> (() => internals.newFrameBoundary(FrameBoundaryType.CurrentRow)) |
    integral ~ (
        PRECEDING ~> ((num: String) =>
          internals.newFrameBoundary(FrameBoundaryType.ValuePreceding, Some(Literal(num)))) |
        FOLLOWING ~> ((num: String) =>
          internals.newFrameBoundary(FrameBoundaryType.ValueFollowing, Some(Literal(num))))
    ) |
    expression ~ (
        PRECEDING ~> ((num: Expression) =>
          internals.newFrameBoundary(FrameBoundaryType.ValuePreceding, Some(num))) |
        FOLLOWING ~> ((num: Expression) =>
          internals.newFrameBoundary(FrameBoundaryType.ValueFollowing, Some(num)))
    )
  }

  protected final def withHints(plan: LogicalPlan): LogicalPlan = {
    if (hasPlanHints) {
      var newPlan = plan
      val planHints = this.planHints
      while (planHints.size() > 0) {
        newPlan match {
          case p if internals.isHintPlan(p) =>
            newPlan = internals.newLogicalPlanWithHints(p, internals.getHints(p) + planHints.pop())
          case _ => newPlan = internals.newLogicalPlanWithHints(plan, Map(planHints.pop()))
        }
      }
      newPlan
    } else plan
  }

  protected final def relation: Rule1[LogicalPlan] = rule {
    relationPrimary ~> (plan => withHints(plan)) ~ (
        joinType.? ~ JOIN ~ (relationPrimary ~> (plan => withHints(plan))) ~ (
            ON ~ expression ~> ((l: LogicalPlan, t: Any, r: LogicalPlan, e: Expression) =>
              withHints(Join(l, r, t.asInstanceOf[Option[JoinType]].getOrElse(Inner), Some(e)))) |
            USING ~ identifierList ~>
                ((l: LogicalPlan, t: Any, r: LogicalPlan, ids: Any) =>
                  withHints(Join(l, r, UsingJoin(t.asInstanceOf[Option[JoinType]]
                      .getOrElse(Inner), ids.asInstanceOf[Seq[String]]), None))) |
            MATCH ~> ((l: LogicalPlan, t: Option[JoinType], r: LogicalPlan) =>
              withHints(Join(l, r, t.getOrElse(Inner), None)))
        ) |
        NATURAL ~ joinType.? ~ JOIN ~ (relationPrimary ~> (plan => withHints(plan))) ~>
            ((l: LogicalPlan, t: Any, r: LogicalPlan) => withHints(Join(l, r,
              NaturalJoin(t.asInstanceOf[Option[JoinType]].getOrElse(Inner)), None)))
    ).*
  }

  protected final def fromClause: Rule1[LogicalPlan] = rule {
    FROM ~ (relation + commaSep) ~ lateralView.* ~ pivot.? ~> ((joins: Seq[LogicalPlan],
        v: Any, createPivot: Any) => {
      val from = if (joins.size == 1) joins.head
      else joins.tail.foldLeft(joins.head) {
        case (lhs, rel) => Join(lhs, rel, Inner, None)
      }
      val views = v.asInstanceOf[Seq[LogicalPlan => LogicalPlan]]
      createPivot.asInstanceOf[Option[LogicalPlan => LogicalPlan]] match {
        case None => views.foldLeft(from) {
          case (child, view) => view(child)
        }
        case Some(f) =>
          if (views.nonEmpty) {
            throw new ParseException("LATERAL cannot be used together with PIVOT in FROM clause")
          }
          f(from)
      }
    })
  }

  protected final def whenClause: Rule1[(Expression, Expression)] = rule {
    WHEN ~ expression ~ THEN ~ expression ~> ((w: Expression, t: Expression) => (w, t))
  }

  protected final def named(e: Expression): NamedExpression = e match {
    case ne: NamedExpression => ne
    case _ => UnresolvedAlias(e)
  }

  // noinspection MutatorLikeMethodIsParameterless
  protected final def setQuantifier: Rule1[Option[Boolean]] = rule {
    (ALL ~ push(true) | DISTINCT ~ push(false)).? ~> ((e: Any) => e.asInstanceOf[Option[Boolean]])
  }

  protected def querySpecification: Rule1[LogicalPlan] = rule {
    SELECT ~ (DISTINCT ~ push(true)).? ~
    PROJECTION_BEGIN ~ namedExpressionSeq ~ PROJECTION_END ~
    fromClause.? ~
    CACHE_LITERAL_BEGIN ~ (WHERE ~ expression).? ~
    groupBy.? ~
    (HAVING ~ expression).? ~ SELECT_STMT_END ~> { (d: Any, p: Seq[Expression], f: Any, w: Any,
        g: Any, h: Any) =>
      val base = f match {
        case Some(plan) =>
          if (_fromRelations.nonEmpty) {
            throw new ParseException("Multi-Insert queries cannot have a FROM clause " +
                "in their individual SELECT statements")
          }
          plan.asInstanceOf[LogicalPlan]
        case _ => if (_fromRelations.isEmpty) internals.newOneRowRelation() else _fromRelations.top
      }
      val withFilter = (child: LogicalPlan) => w match {
        case Some(expr) => Filter(expr.asInstanceOf[Expression], child)
        case _ => child
      }
      val expressions = p.map(named)
      val gr = g.asInstanceOf[Option[(Seq[Expression], Seq[Seq[Expression]], String)]]
      var withProjection = gr match {
        case Some(x) => x._3 match {
          // group by cols with rollup
          case "ROLLUP" => Aggregate(Seq(Rollup(x._1)), expressions, withFilter(base))
          // group by cols with cube
          case "CUBE" => Aggregate(Seq(Cube(x._1)), expressions, withFilter(base))
          // group by cols with grouping sets()()
          case "GROUPINGSETS" => extractGroupingSet(withFilter(base), expressions, x._1, x._2)
          // pivot with group by cols
          case _ if base.isInstanceOf[Pivot] =>
            val newPlan = withFilter(internals.copyPivot(base.asInstanceOf[Pivot],
              groupByExprs = x._1.map(named)))
            if (p.length == 1 && p.head.isInstanceOf[UnresolvedStar]) newPlan
            else Project(expressions, newPlan)
          // just "group by cols"
          case _ => Aggregate(x._1, expressions, withFilter(base))
        }
        case _ =>
          if (expressions.isEmpty) withFilter(base) else Project(expressions, withFilter(base))
      }
      val withHaving = h match {
        case None => withProjection
        case Some(expr) =>
          // Add a cast to non-predicate expressions. If the expression itself is
          // already boolean, the optimizer will get rid of the unnecessary cast.
          val predicate = expr.asInstanceOf[Expression] match {
            case p: Predicate => p
            case e => Cast(e, BooleanType)
          }
          // use Spark 2.4 behaviour for HAVING without GROUP BY
          if (gr.isEmpty &&
              !session.conf.get("spark.sql.legacy.parser.havingWithoutGroupByAsWhere", "false")
                  .equalsIgnoreCase("true")) {
            withProjection = Aggregate(Nil, expressions, withFilter(base))
          }
          internals.newUnresolvedHaving(predicate, withProjection)
      }
      d match {
        case None => withHaving
        case Some(_) => Distinct(withHaving)
      }
    }
  }

  protected final def queryPrimary: Rule1[LogicalPlan] = rule {
    querySpecification |
    O_PAREN ~ queryNoWith ~ C_PAREN |
    // noinspection ConvertibleToMethodValue
    TABLE ~ tableIdentifier ~> (UnresolvedRelation(_)) |
    inlineTable
  }

  /**
   * In Spark 2.3 onwards, INTERSECT has a precedence over other set operators unless
   * [[legacySetOpsPrecedence]] is set (in which case all have the same precedence).
   * This is handled using the intermediate rule below which will then be used in the
   * main rule of [[select]] ensuring that all INTERSECT operators are resolved first.
   */
  private final def queryIntersect: Rule1[LogicalPlan] = rule {
    queryPrimary.named("select") ~ (
        test(!legacySetOpsPrecedence) ~ INTERSECT ~ setQuantifier ~ queryPrimary.named("select") ~>
            ((q1: LogicalPlan, quantifier: Option[Boolean], q2: LogicalPlan) =>
              internals.newIntersect(q1, q2, quantifier.contains(true)))
    ).*
  }

  /**
   * Other set operators UNION, EXCEPT, MINUS (and INTERSECT for legacySetOpsPrecedence)
   */
  protected final def querySetOp: Rule1[LogicalPlan] = rule {
    queryIntersect.named("select") ~ (
        UNION ~ setQuantifier ~ queryIntersect.named("select") ~>
            ((q1: LogicalPlan, quantifier: Option[Boolean], q2: LogicalPlan) =>
              if (quantifier.contains(true)) Union(q1, q2) else Distinct(Union(q1, q2))) |
        test(legacySetOpsPrecedence) ~ INTERSECT ~ setQuantifier ~
            queryIntersect.named("select") ~> ((q1: LogicalPlan, quantifier: Option[Boolean],
            q2: LogicalPlan) => internals.newIntersect(q1, q2, quantifier.contains(true))) |
        (EXCEPT | MINUS) ~ setQuantifier ~ queryIntersect.named("select") ~>
            ((q1: LogicalPlan, quantifier: Option[Boolean], q2: LogicalPlan) =>
              internals.newExcept(q1, q2, quantifier.contains(true)))
    ).*
  }

  /**
   * Full SELECT statement with possible set operations (UNION, INTERSECT etc)
   * and possibly ending with [[queryOrganization]] (ORDER BY, LIMIT etc)
   */
  protected def select: Rule1[LogicalPlan] = rule {
    querySetOp ~ queryOrganization ~> ((q: LogicalPlan, o: LogicalPlan => LogicalPlan) => o(q))
  }

  protected final def query: Rule1[LogicalPlan] = rule {
    queryNoWith | ctes
  }

  protected final def queryNoWith: Rule1[LogicalPlan] = rule {
    select |
    fromClause ~> (_fromRelations.push(_): Unit) ~
        (select | insert | put | delete). + ~> { (queries: Seq[LogicalPlan]) =>
      _fromRelations.pop()
      if (queries.length == 1) queries.head else Union(queries)
    }
  }

  protected final def lateralView: Rule1[LogicalPlan => LogicalPlan] = rule {
    LATERAL ~ VIEW ~ (OUTER ~ push(true)).? ~ functionIdentifier ~ expressionList ~
        identifier ~ (AS.? ~ (identifier + commaSep)).? ~>
        ((o: Any, functionName: FunctionIdentifier, e: Seq[Expression], tableName: String,
            cols: Any) => (child: LogicalPlan) => {
          val columnNames = cols.asInstanceOf[Option[Seq[String]]] match {
            case Some(s) => s.map(UnresolvedAttribute.apply)
            case None => Nil
          }
          internals.newGeneratePlan(UnresolvedGenerator(functionName, e),
            outer = o.asInstanceOf[Option[Boolean]].isDefined, Some(tableName),
            columnNames, child)
        })
  }

  protected final def pivot: Rule1[LogicalPlan => LogicalPlan] = rule {
    PIVOT ~ O_PAREN ~ namedExpressionSeq ~ FOR ~ (identifierList | identifier) ~ IN ~ O_PAREN ~
        CACHE_LITERAL_SKIP ~ namedExpressionSeq ~ CACHE_LITERAL_RESTORE ~ C_PAREN ~ C_PAREN ~>
        ((aggregates: Seq[Expression], ids: Any,
            values: Seq[Expression]) => (child: LogicalPlan) => {
          val pivotColumn = ids match {
            case id: String => UnresolvedAttribute.quoted(id)
            case _ => CreateStruct(ids.asInstanceOf[Seq[String]].map(UnresolvedAttribute.quoted))
          }
          internals.newPivot(Nil, pivotColumn, values, aggregates, child)
        })
  }

  protected final def insert: Rule1[LogicalPlan] = rule {
    INSERT ~ (OVERWRITE ~ push(true) | INTO ~ push(false)) ~ TABLE.? ~
        baseTable ~ partitionSpec ~ ifNotExists ~ select ~> ((overwrite: Boolean,
        r: LogicalPlan, spec: Map[String, Option[String]], exists: Boolean, q: LogicalPlan) =>
      internals.newInsertIntoTable(r, spec, q, overwrite, exists)) |
    INSERT ~ OVERWRITE ~ LOCAL.? ~ DIRECTORY ~ ANY. + ~> (() =>
      sparkParser.parsePlan(input.sliceString(0, input.length)))
  }

  protected final def put: Rule1[LogicalPlan] = rule {
    PUT ~ INTO ~ TABLE.? ~ baseTable ~ select ~> PutIntoTable
  }

  protected final def update: Rule1[LogicalPlan] = rule {
    UPDATE ~ tableIdentifier ~ SET ~ CACHE_LITERAL_BEGIN ~ (((identifier + ('.' ~ ws)) ~
        '=' ~ ws ~ expression ~> ((cols: Seq[String], e: Expression) =>
      UnresolvedAttribute(cols) -> e)) + commaSep) ~ CACHE_LITERAL_END ~
        fromClause.? ~ (WHERE ~ CACHE_LITERAL_BEGIN ~ expression ~ CACHE_LITERAL_END).? ~>
        ((t: TableIdentifier, updateExprs: Seq[(UnresolvedAttribute, Expression)],
            relations: Any, whereExpr: Any) => {
          val table = session.sessionCatalog.resolveRelationWithAlias(t)
          val base = relations match {
            case Some(plan) => plan.asInstanceOf[LogicalPlan]
            case _ => table
          }
          val withFilter = whereExpr match {
            case Some(expr) => Filter(expr.asInstanceOf[Expression], base)
            case _ => base
          }
          val (updateColumns, updateExpressions) = updateExprs.unzip
          Update(table, withFilter, Nil, updateColumns, updateExpressions)
        })
  }

  protected final def delete: Rule1[LogicalPlan] = rule {
    DELETE ~ FROM ~ baseTable ~ (
        WHERE ~ CACHE_LITERAL_BEGIN ~ expression ~ CACHE_LITERAL_END ~>
            ((base: LogicalPlan, expr: Expression) => Delete(base, Filter(expr, base), Nil)) |
        select ~> DeleteFromTable |
        MATCH ~> ((base: LogicalPlan) => Delete(base, base, Nil))
    )
  }

  protected final def ctes: Rule1[LogicalPlan] = rule {
    WITH ~ ((identifier ~ AS.? ~ O_PAREN ~ query ~ C_PAREN ~>
        ((id: String, p: LogicalPlan) => (id, p))) + commaSep) ~ (queryNoWith |
        insert | put | delete) ~> ((r: Seq[(String, LogicalPlan)], s: LogicalPlan) =>
      With(s, r.map(ns => (ns._1,
          // always expect resolution of alias on `query` to be SubqeryAlias
          // and never an UnresolvedRelation
          internals.newSubqueryAlias(ns._1, ns._2).asInstanceOf[SubqueryAlias]))))
  }

  protected def dmlOperation: Rule1[LogicalPlan] = rule {
    capture(INSERT ~ INTO) ~ tableIdentifier ~
        capture(ANY.*) ~> ((c: String, r: TableIdentifier, s: String) => DMLExternalTable(
      UnresolvedRelation(r), s"$c ${quotedUppercaseId(r)} $s"))
  }

  // It can be the following patterns:
  // SHOW TABLES (FROM | IN) schema;
  // SHOW TABLE EXTENDED (FROM | IN) schema ...;
  // SHOW DATABASES;
  // SHOW COLUMNS (FROM | IN) table;
  // SHOW TBLPROPERTIES table;
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  protected def show: Rule1[LogicalPlan] = rule {
    SHOW ~ TABLES ~ ((FROM | IN) ~ identifier).? ~ (LIKE.? ~ stringLiteral).? ~>
        ((id: Any, pat: Any) => new ShowSnappyTablesCommand(
          id.asInstanceOf[Option[String]], pat.asInstanceOf[Option[String]], session)) |
    SHOW ~ TABLE ~ ANY. + ~> (() => sparkParser.parsePlan(input.sliceString(0, input.length))) |
    SHOW ~ VIEWS ~ ((FROM | IN) ~ identifier).? ~ (LIKE.? ~ stringLiteral).? ~>
        ((id: Any, pat: Any) => ShowViewsCommand(session,
          id.asInstanceOf[Option[String]], pat.asInstanceOf[Option[String]])) |
    SHOW ~ (SCHEMAS | DATABASES) ~ (LIKE.? ~ stringLiteral).? ~> ((pat: Any) =>
      ShowDatabasesCommand(pat.asInstanceOf[Option[String]])) |
    SHOW ~ COLUMNS ~ (FROM | IN) ~ tableIdentifier ~ ((FROM | IN) ~ identifier).? ~>
        ((table: TableIdentifier, db: Any) =>
          ShowColumnsCommand(db.asInstanceOf[Option[String]], table)) |
    SHOW ~ TBLPROPERTIES ~ tableIdentifier ~ (O_PAREN ~ optionKey ~ C_PAREN).? ~>
        ((table: TableIdentifier, propertyKey: Any) =>
          ShowTablePropertiesCommand(table, propertyKey.asInstanceOf[Option[String]])) |
    SHOW ~ MEMBERS ~> (() => {
      val newParser = newInstance()
      val servers = if (ClientSharedUtils.isThriftDefault) "THRIFTSERVERS" else "NETSERVERS"
      newParser.parseSQL(
        s"SELECT ID, HOST, KIND, STATUS, $servers, SERVERGROUPS FROM SYS.MEMBERS",
        newParser.querySpecification.run())
    }) |
    SHOW ~ strictIdentifier.? ~ FUNCTIONS ~ (LIKE.? ~
        (functionIdentifier | stringLiteral)).? ~> { (id: Any, nameOrPat: Any) =>
      val (user, system) = id.asInstanceOf[Option[String]]
          .map(Utils.toLowerCase) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) =>
          throw new ParseException(s"SHOW $x FUNCTIONS not supported")
      }
      nameOrPat match {
        case Some(name: FunctionIdentifier) => ShowFunctionsCommand(
          name.database, Some(name.funcName), user, system)
        case Some(pat: String) => ShowFunctionsCommand(
          None, Some(pat), user, system)
        case None => ShowFunctionsCommand(None, None, user, system)
        case _ => throw new ParseException(
          s"SHOW FUNCTIONS $nameOrPat unexpected")
      }
    } |
    SHOW ~ CREATE ~ TABLE ~ tableIdentifier ~> ShowCreateTableCommand
  }

  protected final def explain: Rule1[LogicalPlan] = rule {
    EXPLAIN ~ (EXTENDED ~ push(1) | CODEGEN ~ push(2) | COST ~ push(3)).? ~ sql ~> ((flagVal: Any,
        plan: LogicalPlan) => plan match {
      case _: DescribeTableCommand => ExplainCommand(OneRowRelation.asInstanceOf[LogicalPlan])
      case _ =>
        val flag = flagVal.asInstanceOf[Option[Int]]
        // ensure plan is sent back as CLOB for large plans especially with CODEGEN
        queryHints.put(QueryHint.ColumnsAsClob.toString, "*")
        internals.newExplainCommand(plan, extended = flag.contains(1),
          codegen = flag.contains(2), cost = flag.contains(3))
    })
  }

  protected def analyze: Rule1[LogicalPlan] = rule {
    ANALYZE ~ TABLE ~ tableIdentifier ~ COMPUTE ~ STATISTICS ~
    (FOR ~ COLUMNS ~ (identifier + commaSep) | identifier).? ~>
        ((table: TableIdentifier, ids: Any) => ids.asInstanceOf[Option[Any]] match {
          case None => AnalyzeTableCommand(table, noscan = false)
          case Some(id: String) =>
            if (Utils.toLowerCase(id) != "noscan") {
              throw new ParseException(s"Expected `NOSCAN` instead of `$id`")
            }
            AnalyzeTableCommand(table)
          case Some(cols) => AnalyzeColumnCommand(table, cols.asInstanceOf[Seq[String]])
        })
  }

  protected final def partitionVal: Rule1[(String, Option[String])] = rule {
    identifier ~ ('=' ~ '='.? ~ ws ~ (stringLiterals | numericLiteral |
        booleanLiteral ~> (_.toString) | NULL ~> (() => Consts.NULL.lower))).? ~>
        ((id: String, e: Any) => id -> e.asInstanceOf[Option[String]])
  }

  protected final def partitionSpec: Rule1[Map[String, Option[String]]] = rule {
    (PARTITION ~ O_PAREN ~ (partitionVal + commaSep) ~ C_PAREN).? ~> ((s: Any) =>
      s.asInstanceOf[Option[Seq[(String, Option[String])]]] match {
        case None => Map.empty[String, Option[String]]
        case Some(m) => m.toMap
      })
  }

  private[this] final var cacheLiteral = false
  private[this] final var canCacheLiteral = true
  private[this] final val cacheFlags = new BooleanArrayStack()

  protected final def CACHE_LITERAL_BEGIN: Rule0 = rule {
    MATCH ~> (() => cacheLiteral = canCacheLiteral)
  }

  protected final def CACHE_LITERAL_END: Rule0 = rule {
    MATCH ~> (() => cacheLiteral = false)
  }

  protected final def CACHE_LITERAL_SKIP: Rule0 = rule {
    MATCH ~> { () =>
      cacheFlags.push(canCacheLiteral)
      cacheFlags.push(cacheLiteral)
      cacheLiteral = false
      canCacheLiteral = false
    }
  }

  protected final def CACHE_LITERAL_RESTORE: Rule0 = rule {
    MATCH ~> { () =>
      cacheLiteral = cacheFlags.pop()
      canCacheLiteral = cacheFlags.pop()
    }
  }

  protected final def PROJECTION_BEGIN: Rule0 = rule {
    MATCH ~> { () =>
      _selectContext += 1
      _inProjection = true
      cacheLiteral = canCacheLiteral
    }
  }

  protected final def PROJECTION_END: Rule0 = rule {
    MATCH ~> { () =>
      _inProjection = false
      cacheLiteral = false
    }
  }

  protected final def SELECT_STMT_END: Rule0 = rule {
    MATCH ~> { () =>
      _selectContext -= 1
      _inProjection = _selectContext > 0
      cacheLiteral = canCacheLiteral && _inProjection
    }
  }

  override protected def start: Rule1[LogicalPlan] = rule {
    query.named("select") | insert | put | update | delete | dmlOperation | ddl | show |
    set | reset | cache | uncache | deployPackages | explain | analyze | delegateToSpark
  }

  private final def initConf(): Unit = {
    val conf = session.sessionState.conf
    caseSensitive = conf.caseSensitiveAnalysis
    escapedStringLiterals = conf.getConfString(
      "spark.sql.parser.escapedStringLiterals", "false").equalsIgnoreCase("true")
    sparkCompatible = !Property.HiveCompatibility.get(conf).equalsIgnoreCase("snappy")
    quotedRegexColumnNames = conf.getConfString(
      "spark.sql.parser.quotedRegexColumnNames", "false").equalsIgnoreCase("true")
    legacySetOpsPrecedence = conf.getConfString(
      "spark.sql.legacy.setopsPrecedence.enabled", "false").equalsIgnoreCase("true")
  }

  final def parse[T](sqlText: String, parseRule: => Try[T],
      clearExecutionData: Boolean = false): T = session.synchronized {
    session.clearQueryData()
    if (clearExecutionData) session.snappySessionState.clearExecutionData()
    initConf()
    parseSQL(sqlText, parseRule)
  }

  /** Parse SQL without any other handling like query hints */
  def parseSQLOnly[T](sqlText: String, parseRule: => Try[T]): T = {
    this.input = sqlText
    initConf()
    parseRule match {
      case Success(p) => p
      case Failure(e: ParseError) =>
        throw new ParseException(formatError(e, new ErrorFormatter(showTraces =
            (session ne null) && Property.ParserTraceError.get(session.sessionState.conf))))
      case Failure(e) =>
        throw new ParseException(e.toString, Some(e))
    }
  }

  override protected def parseSQL[T](sqlText: String, parseRule: => Try[T]): T = {
    val plan = parseSQLOnly(sqlText, parseRule)
    if (!queryHints.isEmpty && (session ne null)) {
      session.queryHints.putAll(queryHints)
    }
    plan
  }

  def newInstance(): SnappyParser = new SnappyParser(session)
}
