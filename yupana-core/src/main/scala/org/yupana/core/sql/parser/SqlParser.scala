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

  private def selectWord[_: P] = P(IgnoreCase("SELECT"))
  private def showWord[_: P] = P(IgnoreCase("SHOW"))
  private def tablesWord[_: P] = P(IgnoreCase("TABLES"))
  private def columnsWord[_: P] = P(IgnoreCase("COLUMNS"))
  private def queriesWord[_: P] = P(IgnoreCase("QUERIES"))
  private def queryWord[_: P] = P(IgnoreCase("QUERY"))
  private def fromWord[_: P] = P(IgnoreCase("FROM"))
  private def whereWord[_: P] = P(IgnoreCase("WHERE"))
  private def andWord[_: P] = P(IgnoreCase("AND"))
  private def orWord[_: P] = P(IgnoreCase("OR"))
  private def asWord[_: P] = P(IgnoreCase("AS"))
  private def groupWord[_: P] = P(IgnoreCase("GROUP"))
  private def byWord[_: P] = P(IgnoreCase("BY"))
  private def limitWord[_: P] = P(IgnoreCase("LIMIT"))
  private def caseWord[_: P] = P(IgnoreCase("CASE"))
  private def whenWord[_: P] = P(IgnoreCase("WHEN"))
  private def thenWord[_: P] = P(IgnoreCase("THEN"))
  private def elseWord[_: P] = P(IgnoreCase("ELSE"))
  private def havingWord[_: P] = P(IgnoreCase("HAVING"))
  private def inWord[_: P] = P(IgnoreCase("IN"))
  private def isWord[_: P] = P(IgnoreCase("IS"))
  private def nullWord[_: P] = P(IgnoreCase("NULL"))
  private def notWord[_: P] = P(IgnoreCase("NOT"))
  private def queryIdWord[_: P] = P(IgnoreCase("QUERY_ID"))
  private def stateWord[_: P] = P(IgnoreCase("STATE"))
  private def killWord[_: P] = P(IgnoreCase("KILL"))
  private def deleteWord[_: P] = P(IgnoreCase("DELETE"))
  private def upsertWord[_: P] = P(IgnoreCase("UPSERT"))
  private def intoWord[_: P] = P(IgnoreCase("INTO"))
  private def valuesWord[_: P] = P(IgnoreCase("VALUES"))
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
  private def asterisk[_: P] = P("*")

  private def plus[_: P] = P("+").map(_ => Plus)
  private def minus[_: P] = P("-").map(_ => Minus)
  private def multiply[_: P] = P("*").map(_ => Multiply)
  private def divide[_: P] = P("/").map(_ => Divide)

  def name[_: P]: P[String] = P(CharsWhileIn("a-zA-Z0-9_").!.filter(s => s.nonEmpty && s.exists(_.isLetter)))

  def schemaName[_: P]: P[String] = P(name | "\"" ~ name ~ "\"")
  def fieldWithSchema[_: P]: P[String] = P((schemaName ~ ".").? ~ schemaName).map(_._2)

  def notKeyword[_: P]: P[String] = P(schemaName.filter(s => !keywords.contains(s.toLowerCase)))

  def alias[_: P]: P[String] = P(CharsWhileIn(" \t\n", 1) ~~ asWord.? ~ notKeyword)

  def functionCallExpr[_: P]: P[FunctionCall] = P(name ~ "(" ~ expr.rep(sep = ",") ~ ")").map {
    case (f, vs) => FunctionCall(f.toLowerCase, vs.toList)
  }

  def fieldNameExpr[_: P]: P[FieldName] = P(fieldWithSchema).map(FieldName)

  def constExpr[_: P]: P[Constant] = P(ValueParser.value).map(Constant)

  def caseExpr[_: P]: P[Case] =
    P(
      caseWord ~/
        (whenWord ~/ condition ~ thenWord ~/ expr).rep(1) ~
        elseWord ~/ expr
    ).map { case (cs, d) => Case(cs, d) }

  private def chained[E, _: P](elem: => P[E], op: => P[(E, E) => E]): P[E] = chained1(elem, elem, op)

  private def chained1[E, _: P](firstElem: => P[E], elem: => P[E], op: => P[(E, E) => E]): P[E] = {
    P(firstElem ~ (op ~ elem).rep).map {
      case (first, rest) =>
        rest.foldLeft(first) { case (l, (fun, r)) => fun(l, r) }
    }
  }

  def expr[_: P]: P[SqlExpr] = chained1(minusMathTerm | mathTerm, mathTerm, plus | minus)

  def minusMathTerm[_: P]: P[SqlExpr] = P("-" ~ mathTerm).map(UMinus)

  def mathTerm[_: P]: P[SqlExpr] = chained(mathFactor, multiply | divide)

  def mathFactor[_: P]: P[SqlExpr] = P(functionCallExpr | caseExpr | constExpr | fieldNameExpr | "(" ~ expr ~ ")")

  def field[_: P]: P[SqlField] = P(expr ~~ alias.?).map(SqlField.tupled)

  def fieldList[_: P]: P[SqlFieldList] = P(field.rep(min = 1, sep = ",")).map(SqlFieldList)
  def allFields[_: P]: P[SqlFieldsAll.type] = P(asterisk).map(_ => SqlFieldsAll)

  def op[_: P]: P[(SqlExpr, SqlExpr) => Comparison] = P(
    P("=").map(_ => Eq) |
      P("<>").map(_ => Ne) |
      P("!=").map(_ => Ne) |
      P(">=").map(_ => Ge) |
      P(">").map(_ => Gt) |
      P("<=").map(_ => Le) |
      P("<").map(_ => Lt)
  )

  def callOrField[_: P]: P[SqlExpr] = functionCallExpr | fieldNameExpr

  def comparison[_: P]: P[SqlExpr => Condition] = P(op ~/ expr).map { case (o, b) => a => o(a, b) }

  def in[_: P]: P[SqlExpr => Condition] =
    P(inWord ~/ "(" ~ ValueParser.value.rep(min = 1, sep = ",") ~ ")").map(vs => e => In(e, vs))

  def notIn[_: P]: P[SqlExpr => Condition] =
    P(notWord ~ inWord ~/ "(" ~ ValueParser.value.rep(min = 1, sep = ",") ~ ")").map(vs => e => NotIn(e, vs))

  def isNull[_: P]: P[SqlExpr => Condition] = P(isWord ~ nullWord).map(_ => IsNull)

  def isNotNull[_: P]: P[SqlExpr => Condition] = P(isWord ~ notWord ~ nullWord).map(_ => IsNotNull)

  def condition[_: P]: P[Condition] = P(logicalTerm ~ (orWord ~/ logicalTerm).rep).map {
    case (x, y) if y.nonEmpty => Or(x +: y)
    case (x, _)               => x
  }

  def logicalTerm[_: P]: P[Condition] = P(logicalFactor ~ (andWord ~/ logicalFactor).rep).map {
    case (x, y) if y.nonEmpty => And(x +: y)
    case (x, _)               => x
  }

  def boolExpr[_: P]: P[Condition] = P(expr ~ (comparison | in | notIn | isNull | isNotNull).?).map {
    case (e, Some(f)) => f(e)
    case (e, None)    => ExprCondition(e)
  }

  def logicalFactor[_: P]: P[Condition] = P(boolExpr | ("(" ~ condition ~ ")"))

  def where[_: P]: P[Condition] = P(whereWord ~/ condition)

  def grouping[_: P]: P[SqlExpr] = callOrField

  def groupings[_: P]: P[Seq[SqlExpr]] = P(groupWord ~/ byWord ~ grouping.rep(1, ","))

  def having[_: P]: P[Condition] = P(havingWord ~/ condition)

  def limit[_: P]: P[Int] = P(limitWord ~/ ValueParser.intNumber)

  def selectFields[_: P]: P[SqlFields] = P(selectWord ~/ (fieldList | allFields))

  def nestedSelectFrom[_: P](fields: SqlFields): P[Select] = {
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

  def normalSelectFrom[_: P](fields: SqlFields): P[Select] = {
    P((fromWord ~ schemaName).? ~/ where.? ~ groupings.? ~ having.? ~ limit.?).map {
      case (s, c, gs, h, l) => Select(s, fields, c, gs.getOrElse(Seq.empty), h, l)
    }
  }

  def selectFrom[_: P](fields: SqlFields): P[Select] = {
    P(nestedSelectFrom(fields) | normalSelectFrom(fields))
  }

  def select[_: P]: P[Select] = P(selectFields.flatMap(selectFrom))

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

  def tables[_: P]: P[ShowTables.type] = P(tablesWord).map(_ => ShowTables)

  def columns[_: P]: P[ShowColumns] = P(columnsWord ~/ fromWord ~ schemaName).map(ShowColumns)

  def metricQueryIdFilter[_: P]: P[MetricsFilter] =
    P(queryIdWord ~ "=" ~/ ValueParser.longNumber).map(queryId => MetricsFilter(queryId = Some(queryId)))
  def metricStateFilter[_: P]: P[MetricsFilter] =
    P(stateWord ~ "=" ~/ ValueParser.string).map(state => MetricsFilter(state = Some(state)))
  def queryMetricsFilter[_: P]: P[MetricsFilter] = P(
    whereWord ~ (metricQueryIdFilter | metricStateFilter)
  )

  def queries[_: P]: P[ShowQueryMetrics] =
    P(queriesWord ~/ queryMetricsFilter.? ~/ limit.?).map(ShowQueryMetrics.tupled)

  def query[_: P]: P[KillQuery] = P(queryWord ~/ whereWord ~ metricQueryIdFilter).map(KillQuery)

  def show[_: P]: P[Statement] = P(showWord ~/ (columns | tables | queries))

  def kill[_: P]: P[Statement] = P(killWord ~/ query)

  def delete[_: P]: P[DeleteQueryMetrics] =
    P(deleteWord ~/ queriesWord ~/ queryMetricsFilter).map(DeleteQueryMetrics)

  def upsertFields[_: P]: P[Seq[String]] = "(" ~/ fieldWithSchema.rep(min = 1, sep = ",") ~ ")"

  def values[_: P](count: Int): P[Seq[Seq[SqlExpr]]] =
    ("(" ~/ expr.rep(exactly = count, sep = ",") ~ ")").opaque(s"<$count expressions>").rep(1, ",")

  def upsert[_: P]: P[Upsert] =
    P(upsertWord ~ intoWord ~/ schemaName ~/ upsertFields ~ valuesWord).flatMap {
      case (table, fields) => values(fields.size).map(vs => Upsert(table, fields, vs))
    }

  def statement[_: P]: P[Statement] = P((select | upsert | show | kill | delete) ~ ";".? ~ End)

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
