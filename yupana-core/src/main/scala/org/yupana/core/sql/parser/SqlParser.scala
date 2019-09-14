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

import fastparse.all._
import fastparse.core.Parsed.{Failure, Success}

class SqlParser extends ValueParser {
  import white._

  private val selectWord = P(IgnoreCase("SELECT"))
  private val showWord = P(IgnoreCase("SHOW"))
  private val tablesWord = P(IgnoreCase("TABLES"))
  private val columnsWord = P(IgnoreCase("COLUMNS"))
  private val fromWord = P(IgnoreCase("FROM"))
  private val whereWord = P(IgnoreCase("WHERE"))
  private val andWord = P(IgnoreCase("AND"))
  private val orWord = P(IgnoreCase("OR"))
  private val asWord = P(IgnoreCase("AS"))
  private val groupWord = P(IgnoreCase("GROUP"))
  private val byWord = P(IgnoreCase("BY"))
  private val limitWord = P(IgnoreCase("LIMIT"))
  private val caseWord = P(IgnoreCase("CASE"))
  private val whenWord = P(IgnoreCase("WHEN"))
  private val thenWord = P(IgnoreCase("THEN"))
  private val elseWord = P(IgnoreCase("ELSE"))
  private val havingWord = P(IgnoreCase("HAVING"))
  private val inWord = P(IgnoreCase("IN"))
  private val isWord = P(IgnoreCase("IS"))
  private val nullWord = P(IgnoreCase("NULL"))
  private val notWord = P(IgnoreCase("NOT"))
  private val keywords = Set(
    "select", "from", "where", "and", "or", "as", "group", "order", "by", "limit", "case", "when", "then", "else",
    "having", "in", "is", "null", "not"
  )
  private val asterisk = P("*")

  private val plus = P("+").map(_ => Plus)
  private val minus = P("-"). map(_ => Minus)
  private val multiply = P("*").map(_ => Multiply)
  private val divide = P("/").map(_ => Divide)

  def wrapOpt[T](open: String, close: String, p: Parser[T]): Parser[T] = P(p | open ~ p ~ close)

  val name: Parser[String] = P(CharsWhileIn(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') :+ '_').!
    .filter(s => s.nonEmpty && s.exists(_.isLetter)))

  val schemaName: Parser[String] = wrapOpt("\"", "\"", name)
  val fieldWithSchema: Parser[String] = P((schemaName ~ ".").? ~ schemaName).map(_._2)

  val notKeyword: Parser[String] = P(schemaName.filter(s => !keywords.contains(s.toLowerCase)))

  val alias: Parser[String] = P(asWord.? ~ notKeyword)

  val functionCallExpr: Parser[FunctionCall] = P(name ~ "(" ~ expr.rep(sep =",") ~ ")").map { case (f, vs) => FunctionCall(f.toLowerCase, vs.toList) }

  val fieldNameExpr: Parser[FieldName] = P(fieldWithSchema).map(FieldName)

  val constExpr: Parser[Constant] = P(value).map(Constant)

  val caseExpr: Parser[Case] = P(
      caseWord ~/
        (whenWord ~/ condition ~ thenWord ~/ expr).rep(min = 1) ~
        elseWord ~/ expr
  ).map { case (cs, d) => Case(cs, d) }

  private def chained[E](elem: Parser[E], op: Parser[(E, E) => E]): Parser[E] = chained1(elem, elem, op)

  private def chained1[E](firstElem: Parser[E], elem: Parser[E], op: Parser[(E, E) => E]): Parser[E] = {
    P(firstElem ~ (op ~ elem).rep).map { case (first, rest) =>
      rest.foldLeft(first) { case (l, (fun, r)) => fun(l, r) }
    }
  }

  def expr: Parser[SqlExpr] = chained1(minusMathTerm | mathTerm, mathTerm, plus | minus)

  def minusMathTerm: Parser[SqlExpr] = P("-" ~ mathTerm).map(UMinus)

  def mathTerm: Parser[SqlExpr] = chained(mathFactor, multiply | divide)

  def mathFactor: Parser[SqlExpr] = P(functionCallExpr | caseExpr | constExpr | fieldNameExpr | "(" ~ expr ~ ")")

  val field: Parser[SqlField] = P(expr ~ alias.?).map(SqlField.tupled)

  val fieldList: Parser[SqlFieldList] = P(field.rep(min = 1, sep = ",")).map(SqlFieldList)
  val allFields: Parser[SqlFieldsAll.type] = P(asterisk).map(_ => SqlFieldsAll)

  val op: Parser[(SqlExpr, SqlExpr) => Comparison] = P(
    P("=").map(_ => Eq) |
    P("<>").map(_ => Ne) |
    P("!=").map( _ => Ne) |
    P(">=").map(_ => Ge) |
    P(">").map(_ => Gt) |
    P("<=").map(_ => Le) |
    P("<").map(_ => Lt)
  )

  def callOrField: Parser[SqlExpr] = functionCallExpr | fieldNameExpr

  val comparison: Parser[SqlExpr => Condition] = P(op ~/ expr).map { case (o, b) => a => o(a, b) }

  val in: Parser[SqlExpr => Condition] = P(inWord ~/ "(" ~ value.rep(min = 1, sep = ",") ~ ")").map(vs => e => In(e, vs))

  val notIn: Parser[SqlExpr => Condition] = P(notWord ~ inWord ~/ "(" ~ value.rep(min = 1, sep = ",") ~ ")").map(vs => e => NotIn(e, vs))

  val isNull: Parser[SqlExpr => Condition] = P(isWord ~ nullWord).map(_ => IsNull)

  val isNotNull: Parser[SqlExpr => Condition] = P(isWord ~ notWord ~ nullWord).map(_ => IsNotNull)

  def condition: Parser[Condition] = P(logicalTerm ~ (orWord ~/ logicalTerm).rep).map {
    case (x, y) if y.nonEmpty => Or(x +: y)
    case (x, _) => x
  }

  def logicalTerm: Parser[Condition] = P(logicalFactor ~ (andWord ~/ logicalFactor).rep).map {
    case (x, y) if y.nonEmpty => And(x +: y)
    case (x, _) => x
  }

  def boolExpr: Parser[Condition] = P(expr ~ (comparison | in | notIn | isNull | isNotNull).?).map {
    case (e, Some(f)) => f(e)
    case (e, None) => ExprCondition(e)
  }

  def logicalFactor: Parser[Condition] = P(boolExpr | ("(" ~ condition ~ ")"))

  def where: Parser[Condition] = P(whereWord ~/ condition)

  def grouping: Parser[SqlExpr] = callOrField

  def groupings: Parser[Seq[SqlExpr]] = P(groupWord ~/ byWord ~ grouping.rep(1, ","))

  def having: Parser[Condition] = P(havingWord ~/ condition)

  val limit: Parser[Int] = P(limitWord ~/ intNumber)

  val selectFields: Parser[SqlFields] = P(selectWord ~/ (fieldList | allFields))

  def nestedSelectFrom(fields: SqlFields): Parser[Select] = {
    P("(" ~ select ~ ")" ~/ (asWord.? ~ notKeyword).?).map {
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

  def normalSelectFrom(fields: SqlFields): Parser[Select] = {
    P(schemaName ~/ where.? ~ groupings.? ~ having.? ~ limit.?).map {
      case (s, c, gs, h, l) => Select(s, fields, c, gs.getOrElse(Seq.empty), h, l)
    }
  }

  def selectFrom(fields: SqlFields): Parser[Select] = {
    // Pass is required because FastParse 1.0.0 bug https://github.com/lihaoyi/fastparse/issues/88
    P(Pass ~ fromWord ~/ (nestedSelectFrom(fields) | normalSelectFrom(fields)))
  }

  val select: Parser[Select] = P(selectFields.flatMap(selectFrom))

  def substituteNested(expr: SqlExpr, nestedFields: Seq[SqlField]): SqlExpr = {
    expr match {
      case e@FieldName(n) =>
        nestedFields.find(f => f.alias.orElse(f.expr.proposedName).contains(n)).map(_.expr).getOrElse(e)

      case FunctionCall(n, es) => FunctionCall(n, es.map(e => substituteNested(e, nestedFields)))

      case Plus(a, b) => Plus(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Minus(a, b) => Minus(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Multiply(a, b) => Multiply(substituteNested(a, nestedFields), substituteNested(b, nestedFields))
      case Divide(a, b) => Divide(substituteNested(a, nestedFields), substituteNested(b, nestedFields))

      case e => e
    }
  }

  val tables: Parser[ShowTables.type] = P(tablesWord).map(_ => ShowTables)

  val columns: Parser[ShowColumns] = P(columnsWord ~/ fromWord ~ schemaName).map(ShowColumns)

  val show: Parser[Statement] = P(showWord ~/ (columns | tables))

  val statement: Parser[Statement] = P((select | show) ~ ";".? ~ End)

  def parse(sql: String): Either[String, Statement] = {
    statement.parse(sql.trim) match {
      case Success(s, _)      => Right(s)
      case Failure(lastParser, index, extra) =>
        val expected = lastParser.toString.toUpperCase
        val actual = extra.input.repr.literalize(extra.input.slice(index, index + 10))
        val position = extra.input.repr.prettyIndex(extra.input, index)

        Left(
          s"""Invalid SQL statement: '$sql'
             |Expect $expected, but got $actual at $position""".stripMargin
        )
    }
  }
}

object SqlParser {
  def parse(sql: String): Either[String, Statement] = {
    val parser = new SqlParser
    parser.parse(sql)
  }
}
