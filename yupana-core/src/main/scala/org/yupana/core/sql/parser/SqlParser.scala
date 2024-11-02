/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.sql.parser

import fastparse._
import fastparse.internal.Util
import MultiLineWhitespace._

object SqlParser {

  private def selectWord[$: P] = P(IgnoreCase("SELECT"))
  private def showWord[$: P] = P(IgnoreCase("SHOW"))
  private def tablesWord[$: P] = P(IgnoreCase("TABLES"))
  private def columnsWord[$: P] = P(IgnoreCase("COLUMNS"))
  private def queriesWord[$: P] = P(IgnoreCase("QUERIES"))
  private def updatesIntervalsWord[$: P] = P(IgnoreCase("UPDATES_INTERVALS"))
  private def queryWord[$: P] = P(IgnoreCase("QUERY"))
  private def fromWord[$: P] = P(IgnoreCase("FROM"))
  private def whereWord[$: P] = P(IgnoreCase("WHERE"))
  private def andWord[$: P] = P(IgnoreCase("AND"))
  private def orWord[$: P] = P(IgnoreCase("OR"))
  private def asWord[$: P] = P(IgnoreCase("AS"))
  private def betweenWord[$: P] = P(IgnoreCase("BETWEEN"))
  private def groupWord[$: P] = P(IgnoreCase("GROUP"))
  private def byWord[$: P] = P(IgnoreCase("BY"))
  private def limitWord[$: P] = P(IgnoreCase("LIMIT"))
  private def caseWord[$: P] = P(IgnoreCase("CASE"))
  private def whenWord[$: P] = P(IgnoreCase("WHEN"))
  private def thenWord[$: P] = P(IgnoreCase("THEN"))
  private def elseWord[$: P] = P(IgnoreCase("ELSE"))
  private def havingWord[$: P] = P(IgnoreCase("HAVING"))
  private def inWord[$: P] = P(IgnoreCase("IN"))
  private def isWord[$: P] = P(IgnoreCase("IS"))
  private def nullWord[$: P] = P(IgnoreCase("NULL"))
  private def notWord[$: P] = P(IgnoreCase("NOT"))
  private def castWord[$: P] = P(IgnoreCase("CAST"))
  private def queryIdWord[$: P] = P(IgnoreCase("QUERY_ID"))
  private def stateWord[$: P] = P(IgnoreCase("STATE"))
  private def killWord[$: P] = P(IgnoreCase("KILL"))
  private def deleteWord[$: P] = P(IgnoreCase("DELETE"))
  private def upsertWord[$: P] = P(IgnoreCase("UPSERT"))
  private def intoWord[$: P] = P(IgnoreCase("INTO"))
  private def valuesWord[$: P] = P(IgnoreCase("VALUES"))
  private def functionsWord[$: P] = P(IgnoreCase("FUNCTIONS"))
  private def forWord[$: P] = P(IgnoreCase("FOR"))
  private val keywords = Set(
    "select",
    "from",
    "where",
    "and",
    "or",
    "as",
    "group",
    "order",
    "by",
    "limit",
    "case",
    "when",
    "then",
    "else",
    "having",
    "in",
    "is",
    "null",
    "not"
  )
  private def asterisk[$: P] = P("*")

  private def plus[$: P] = P("+").map(_ => Plus)
  private def minus[$: P] = P("-").map(_ => Minus)
  private def multiply[$: P] = P("*").map(_ => Multiply)
  private def divide[$: P] = P("/").map(_ => Divide)

  def name[$: P]: P[String] = P(CharsWhileIn("a-zA-Z0-9_").!.filter(s => s.nonEmpty && s.exists(_.isLetter)))

  def schemaName[$: P]: P[String] = P(name | "\"" ~ name ~ "\"")
  def fieldWithSchema[$: P]: P[String] = P((schemaName ~ ".").? ~ schemaName).map(_._2)

  def notKeyword[$: P]: P[String] = P(schemaName.filter(s => !keywords.contains(s.toLowerCase)))

  def alias[$: P]: P[String] = P(CharsWhileIn(" \t\n", 1) ~~ asWord.? ~ notKeyword)

  def functionCallExpr[$: P]: P[FunctionCall] = P(name ~ "(" ~ expr.rep(sep = ",") ~ ")").map {
    case (f, vs) => FunctionCall(f.toLowerCase, vs.toList)
  }

  def fieldNameExpr[$: P]: P[FieldName] = P(fieldWithSchema).map(FieldName)

  def constExpr[$: P]: P[Constant] = P(ValueParser.value).map(Constant)

  def arrayExpr[$: P]: P[SqlArray] = P("{" ~ ValueParser.value.rep(min = 1, sep = ",") ~ "}").map(SqlArray)

  def caseExpr[$: P]: P[Case] =
    P(
      caseWord ~/
        (whenWord ~/ condition ~ thenWord ~/ expr).rep(1) ~
        elseWord ~/ expr
    ).map { case (cs, d) => Case(cs, d) }

  private def chained[E, $: P](elem: => P[E], op: => P[(E, E) => E]): P[E] = chained1(elem, elem, op)

  private def chained1[E, $: P](firstElem: => P[E], elem: => P[E], op: => P[(E, E) => E]): P[E] = {
    P(firstElem ~ (op ~ elem).rep).map {
      case (first, rest) =>
        rest.foldLeft(first) { case (l, (fun, r)) => fun(l, r) }
    }
  }

  def mathExpr[$: P]: P[SqlExpr] = chained1(minusMathTerm | mathTerm, mathTerm, plus | minus)

  def expr[$: P]: P[SqlExpr] = P(condition)

  def minusMathTerm[$: P]: P[SqlExpr] = P("-" ~ mathTerm).map(UMinus)

  def mathTerm[$: P]: P[SqlExpr] = chained(mathFactor, multiply | divide)

  def exprOrTuple[$: P]: P[SqlExpr] = P("(" ~ expr ~ ("," ~ expr).? ~ ")").map {
    case (a, Some(b)) => Tuple(a, b)
    case (e, None)    => e
  }

  def mathFactor[$: P]: P[SqlExpr] =
    P(castExpr | functionCallExpr | caseExpr | constExpr | arrayExpr | fieldNameExpr | exprOrTuple)

  def dataType[$: P]: P[String] = P(CharsWhileIn("a-zA-Z").!)

  def castExpr[$: P]: P[SqlExpr] = P(castWord ~ "(" ~/ expr ~ asWord ~ dataType ~ ")").map(CastExpr.tupled)

  def field[$: P]: P[SqlField] = P(expr ~~ alias.?).map(SqlField.tupled)

  def fieldList[$: P]: P[SqlFieldList] = P(field.rep(min = 1, sep = ",")).map(SqlFieldList)
  def allFields[$: P]: P[SqlFieldsAll.type] = P(asterisk).map(_ => SqlFieldsAll)

  def op[$: P]: P[(SqlExpr, SqlExpr) => Comparison] = P(
    P(">=").map(_ => Ge) |
      P(">").map(_ => Gt) |
      P("<=").map(_ => Le) |
      P("<" ~ !">").map(_ => Lt)
  )

  def eqOp[$: P]: P[(SqlExpr, SqlExpr) => Comparison] = P(
    P("=").map(_ => Eq) |
      P("<>").map(_ => Ne) |
      P("!=").map(_ => Ne)
  )

  def callOrField[$: P]: P[SqlExpr] = functionCallExpr | fieldNameExpr

  def comparison[$: P]: P[SqlExpr => SqlExpr] = P(op ~/ mathExpr).map { case (o, b) => a => o(a, b) }

  def equasion[$: P]: P[SqlExpr] = P(boolExpr ~ (eqOp ~/ boolExpr).?).map {
    case (a, Some((o, b))) => o(a, b)
    case (a, None)         => a
  }

  def in[$: P]: P[SqlExpr => SqlExpr] =
    P(inWord ~/ "(" ~ ValueParser.value.rep(min = 1, sep = ",") ~ ")").map(vs => e => In(e, vs))

  def notIn[$: P]: P[SqlExpr => SqlExpr] =
    P(notWord ~ inWord ~/ "(" ~ ValueParser.value.rep(min = 1, sep = ",") ~ ")").map(vs => e => NotIn(e, vs))

  def isNull[$: P]: P[SqlExpr => SqlExpr] = P(isWord ~ nullWord).map(_ => IsNull)

  def isNotNull[$: P]: P[SqlExpr => SqlExpr] = P(isWord ~ notWord ~ nullWord).map(_ => IsNotNull)

  def between[$: P]: P[SqlExpr => SqlExpr] = P(betweenWord ~/ ValueParser.value ~ andWord ~/ ValueParser.value).map {
    case (f, t) => e => BetweenCondition(e, f, t)
  }

  def condition[$: P]: P[SqlExpr] = P(logicalTerm ~ (orWord ~/ logicalTerm).rep).map {
    case (x, y) if y.nonEmpty => Or(x +: y)
    case (x, _)               => x
  }

  def logicalTerm[$: P]: P[SqlExpr] = P(logicalFactor ~ (andWord ~/ logicalFactor).rep).map {
    case (x, y) if y.nonEmpty => And(x +: y)
    case (x, _)               => x
  }

  def boolExpr[$: P]: P[SqlExpr] = P(mathExpr ~ (comparison | in | notIn | isNull | isNotNull | between).?).map {
    case (e, Some(f)) => f(e)
    case (e, None)    => e
  }

  def logicalFactor[$: P]: P[SqlExpr] = P(equasion | ("(" ~ condition ~ ")"))

  def where[$: P]: P[SqlExpr] = P(whereWord ~/ condition)

  def grouping[$: P]: P[SqlExpr] = callOrField

  def groupings[$: P]: P[Seq[SqlExpr]] = P(groupWord ~/ byWord ~ grouping.rep(1, ","))

  def having[$: P]: P[SqlExpr] = P(havingWord ~/ condition)

  def limit[$: P]: P[Int] = P(limitWord ~/ ValueParser.intNumber)

  def selectFields[$: P]: P[SqlFields] = P(selectWord ~/ (fieldList | allFields))

  def nestedSelectFrom[$: P](fields: SqlFields): P[Select] = {
    P(fromWord ~ "(" ~/ select ~ ")" ~/ (asWord.? ~ notKeyword).?).map {
      case (sel, _) =>
        fields match {
          case SqlFieldsAll => sel

          case SqlFieldList(fs) =>
            val newFields = sel.fields match {
              case SqlFieldList(inner) =>
                fs.map(f => f.copy(expr = substituteNested(f.expr, inner)))

              case SqlFieldsAll => fs
            }
            sel.copy(fields = SqlFieldList(newFields))
        }
    }
  }

  def normalSelectFrom[$: P](fields: SqlFields): P[Select] = {
    P((fromWord ~ schemaName).? ~/ where.? ~ groupings.? ~ having.? ~ limit.?).map {
      case (s, c, gs, h, l) => Select(s, fields, c, gs.getOrElse(Seq.empty), h, l)
    }
  }

  def selectFrom[$: P](fields: SqlFields): P[Select] = {
    P(nestedSelectFrom(fields) | normalSelectFrom(fields))
  }

  def select[$: P]: P[Select] = P(selectFields.flatMap(selectFrom(_)) ~/ where.?) map {
    case (sel, Some(cond)) => sel.copy(condition = Some(andConditions(sel.condition, cond)))
    case (sel, None)       => sel
  }

  private def andConditions(filter: Option[SqlExpr], extra: SqlExpr): SqlExpr = {
    filter match {
      case Some(And(cs)) => And(cs :+ extra)
      case Some(c)       => And(Seq(c, extra))
      case None          => extra
    }
  }

  def substituteNested(expr: SqlExpr, nestedFields: Seq[SqlField]): SqlExpr = {
    expr match {
      case e @ FieldName(n) =>
        nestedFields.find(f => f.alias.orElse(f.expr.proposedName).contains(n)).map(_.expr).getOrElse(e)

      case FunctionCall(n, es) => FunctionCall(n, es.map(e => substituteNested(e, nestedFields)))

      case Plus(a, b)     => Plus(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Minus(a, b)    => Minus(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Multiply(a, b) => Multiply(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Divide(a, b)   => Divide(substituteNested(a, nestedFields), substituteNested(b, nestedFields))

      case e => e
    }
  }

  def tables[$: P]: P[ShowTables.type] = P(tablesWord).map(_ => ShowTables)

  def columns[$: P]: P[ShowColumns] = P(columnsWord ~/ fromWord ~ schemaName).map(ShowColumns)

  def metricQueryIdFilter[$: P]: P[MetricsFilter] =
    P(queryIdWord ~ "=" ~/ ValueParser.string).map(queryId => MetricsFilter(queryId = Some(queryId)))
  def metricStateFilter[$: P]: P[MetricsFilter] =
    P(stateWord ~ "=" ~/ ValueParser.string).map(state => MetricsFilter(state = Some(state)))
  def queryMetricsFilter[$: P]: P[MetricsFilter] = P(
    whereWord ~ (metricQueryIdFilter | metricStateFilter)
  )

  def queries[$: P]: P[ShowQueryMetrics] =
    P(queriesWord ~/ queryMetricsFilter.? ~/ limit.?).map(ShowQueryMetrics.tupled)

  def updatesIntervals[$: P]: P[ShowUpdatesIntervals] =
    P(updatesIntervalsWord ~ (whereWord ~ condition).?).map(ShowUpdatesIntervals)

  def query[$: P]: P[KillQuery] = P(queryWord ~/ whereWord ~ metricQueryIdFilter).map(KillQuery)

  def functions[$: P]: P[ShowFunctions] = P(functionsWord ~/ forWord ~ name).map(ShowFunctions)

  def show[$: P]: P[Statement] =
    P(showWord ~/ (columns | tables | queries | functions | updatesIntervals))

  def kill[$: P]: P[Statement] = P(killWord ~/ query)

  def delete[$: P]: P[DeleteQueryMetrics] =
    P(deleteWord ~/ queriesWord ~/ queryMetricsFilter).map(DeleteQueryMetrics)

  def upsertFields[$: P]: P[Seq[String]] = "(" ~/ fieldWithSchema.rep(min = 1, sep = ",") ~ ")"

  def values[$: P](count: Int): P[Seq[Seq[SqlExpr]]] =
    ("(" ~/ expr.rep(exactly = count, sep = ",") ~ ")").opaque(s"<$count expressions>").rep(1, ",")

  def upsert[$: P]: P[Upsert] =
    P(upsertWord ~ intoWord ~/ schemaName ~/ upsertFields ~ valuesWord).flatMap {
      case (table, fields) => values(fields.size).map(vs => Upsert(table, fields, vs))
    }

  def statement[$: P]: P[Statement] = P((select | upsert | show | kill | delete) ~ ";".? ~ End)

  def parse(sql: String): Either[String, Statement] = {
    fastparse.parse(sql.trim, statement(_)) match {
      case Parsed.Success(s, _) => Right(s)
      case f @ Parsed.Failure(_, index, extra) =>
        val trace = f.trace()
        val expected = trace.terminals.render
        val actual = Util.literalize(extra.input.slice(index, index + 10))
        val position = extra.input.prettyIndex(index)

        Left(
          s"""Invalid SQL statement: '$sql'
             |Expect $expected, but got $actual at $position""".stripMargin
        )
    }
  }
}
