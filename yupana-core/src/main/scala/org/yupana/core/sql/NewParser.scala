package org.yupana.core.sql

import fastparse._
import fastparse.MultiLineWhitespace._
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.Schema
import org.yupana.api.types.DataType

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

  def constExpr[_: P]: P[Expression] = NewValueParser.value

  private def chained[E, _: P](elem: => P[E], op: => P[(E, E) => E]): P[E] = chained1(elem, elem, op)

  private def chained1[E, _: P](firstElem: => P[E], elem: => P[E], op: => P[(E, E) => E]): P[E] = {
    P(firstElem ~ (op ~ elem).rep).map {
      case (first, rest) =>
        rest.foldLeft(first) { case (l, (fun, r)) => fun(l, r) }
    }
  }

  private def plus[_: P] = P("+").map(_ => PlusExpr)
  private def minus[_: P] = P("-").map(_ => MinusExpr)
  private def multiply[_: P] = P("*").map(_ => TimesExpr)
  private def divide[_: P] = P("/").map(_ => DivIntExpr)

  def expr[_: P]: P[Expression] = chained1(minusMathTerm | mathTerm, mathTerm, plus | minus)

  def minusMathTerm[_: P]: P[Expression] = P("-" ~ mathTerm).map(x => UnaryMinusExpr(x.aux))

  def mathTerm[_: P]: P[Expression] = chained(mathFactor, multiply | divide)

  def mathFactor[_: P]: P[Expression] =
    P(functionCallExpr | /* caseExpr | */ constExpr | fieldNameExpr | "(" ~ expr ~ ")")

  def fieldNameExpr[_: P]: P[Expression] = fieldWithSchema.flatMap(recognizeField)

  def field[_: P]: P[QueryField] = P(expr ~~ alias.?).map {
    case (e, a) =>
      QueryField(a.getOrElse("FIXME"), e)
  } // FIXME

  def functionCallExpr[_: P]: P[Expression] =
    unary("trunkYear", DataType[Time], TrunkYearExpr.apply) |
      unary("trunkMonth", DataType[Time], TrunkMonthExpr.apply) |
      unary("trunkDay", DataType[Time], TrunkDayExpr.apply) |
      unary("trunkHour", DataType[Time], TrunkHourExpr.apply) |
      unary("trunkMinute", DataType[Time], TrunkMinuteExpr.apply) |
      unary("trunkSecond", DataType[Time], TrunkSecondExpr.apply)

  private def unary[T, _: P](
      fn: String,
      tpe: DataType.Aux[T],
      create: Expression.Aux[T] => Expression
  ): P[Expression] = {
    P(fn ~ "(" ~ expr ~ ")").flatMap { e =>
      if (e.dataType == tpe)
        Pass(create(e.asInstanceOf[Expression.Aux[T]]))
      else Fail(s"Function $fn can not be applied to ${e.dataType}")
    }
  }

  private def recognizeField[_: P](name: String): P[Expression] = ???
}

//object NewParser {
//
//  private def convertValue[_: P](v: parser.Value /*, exprType: ExprType*/ ): P[ConstantExpr] = {
//    v match {
//      case parser.StringValue(s) =>
////        val const = if (exprType == ExprType.Cmp) s.toLowerCase else s
//        val const = s
//        Pass(ConstantExpr(const))
//
//      case parser.NumericValue(n) => Pass(ConstantExpr(n))
//
//      case parser.TimestampValue(t) => Pass(ConstantExpr(Time(t)))
//
//      case parser.PeriodValue(p) => //if exprType == ExprType.Cmp =>
////        if (p.getYears == 0 && p.getMonths == 0) {
//        Pass(ConstantExpr(p.toStandardDuration.getMillis))
////        } else {
////          Left(s"Period $p cannot be used as duration, because it has months or years")
////        }
//
//      case parser.PeriodValue(p) => Pass(ConstantExpr(p))
//
//      case parser.Placeholder =>
////        state.nextPlaceholderValue().right.flatMap(v => convertValue(state, v, exprType))
//    }
//  }
//
//}
