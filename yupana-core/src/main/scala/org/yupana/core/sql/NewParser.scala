package org.yupana.core.sql

import fastparse._
import fastparse.MultiLineWhitespace._
import org.yupana.api.Time
import org.yupana.api.query.ConstantExpr
import org.yupana.api.schema.Schema
import org.yupana.core.sql.parser.ValueParser

class NewParser(schema: Schema) {
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

  def name[_: P]: P[String] = P(CharsWhileIn("a-zA-Z0-9_").!.filter(s => s.nonEmpty && s.exists(_.isLetter)))
  def schemaName[_: P]: P[String] = P(name | "\"" ~ name ~ "\"")
  def fieldWithSchema[_: P]: P[String] = P((schemaName ~ ".").? ~ schemaName).map(_._2)

  def notKeyword[_: P]: P[String] = P(schemaName.filter(s => !keywords.contains(s.toLowerCase)))

  def alias[_: P]: P[String] = P(CharsWhileIn(" \t\n", 1) ~~ asWord.? ~ notKeyword)

  // Expressions

  def constExpr[_: P]: P[ConstantExpr] = P(ValueParser.value).map(ConstantExpr.apply)

}

object NewParser {

  private def convertValue(v: parser.Value /*, exprType: ExprType*/ ): Either[String, ConstantExpr] = {
    v match {
      case parser.StringValue(s) =>
//        val const = if (exprType == ExprType.Cmp) s.toLowerCase else s
        val const = s
        Right(ConstantExpr(const))

      case parser.NumericValue(n) =>
        Right(ConstantExpr(n))

      case parser.TimestampValue(t) =>
        Right(ConstantExpr(Time(t)))

      case parser.PeriodValue(p) //if exprType == ExprType.Cmp =>
//        if (p.getYears == 0 && p.getMonths == 0) {
          Right(ConstantExpr(p.toStandardDuration.getMillis))
//        } else {
//          Left(s"Period $p cannot be used as duration, because it has months or years")
//        }

      case parser.PeriodValue(p) => Right(ConstantExpr(p))

      case parser.Placeholder =>
//        state.nextPlaceholderValue().right.flatMap(v => convertValue(state, v, exprType))
    }
  }

}
