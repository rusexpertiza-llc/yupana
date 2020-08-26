package org.yupana.core.sql

import fastparse._
import fastparse.MultiLineWhitespace._
import fastparse.internal.Util
import org.joda.time.Period
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Schema, Table }
import org.yupana.api.types.DataType
import org.yupana.api.utils.CollectionUtils

import scala.language.higherKinds

sealed trait SqlFields
case class SqlFieldList(fields: Seq[QueryField]) extends SqlFields
case object SqlFieldsAll extends SqlFields

class NewParser(schema: Schema) {
  type Recognizer = String => Either[String, Expression]
  type NamedExpr = (Expression, String)

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
  def tableName[_: P]: P[String] = P(name | "\"" ~ name ~ "\"")
  def fieldWithSchema[_: P]: P[String] = P((tableName ~ ".").? ~ tableName).map(_._2)

  def notKeyword[_: P]: P[String] = P(tableName.filter(s => !keywords.contains(s.toLowerCase)))
//F IXME LATER
//  def alias[_: P]: P[String] = P(CharsWhileIn(" \t\n", 1) ~~ asWord.? ~ notKeyword)
  def alias[_: P]: P[String] = P(asWord.? ~ notKeyword)

  // Expressions
  def numericConst[_: P]: P[(ConstantExpr, String)] = P(NewValueParser.number).map(x => ConstantExpr(x) -> x.toString)
  def stringConst[_: P]: P[(ConstantExpr, String)] = P(NewValueParser.string).map(x => ConstantExpr(x) -> x)
  def timestampConst[_: P]: P[(ConstantExpr, String)] = P(NewValueParser.timestamp).map { ts =>
    val time = Time(ts)
    ConstantExpr(time) -> time.toLocalDateTime.toString
  }
  def periodConst[_: P]: P[(ConstantExpr, String)] = NewValueParser.period.map(p => ConstantExpr(p) -> p.toString)
  def durationConst[_: P]: P[(ConstantExpr, String)] =
    NewValueParser.period.flatMap(p =>
      if (p.getYears == 0 && p.getMonths == 0) {
        val dur = p.toStandardDuration
        Pass(ConstantExpr(dur.getMillis) -> dur.toString)
      } else {
        Fail(s"Period $p cannot be used as duration, because it has months or years")
      }
    )

  def constExpr[_: P]: P[(ConstantExpr, String)] =
    P(numericConst | stringConst | timestampConst | periodConst | durationConst)

  private def chained[E, _: P](elem: => P[E], op: => P[(E, E) => P[E]]): P[E] = chained1(elem, elem, op)

  private def chained1[E, _: P](firstElem: => P[E], elem: => P[E], op: => P[(E, E) => P[E]]): P[E] = {
    def p = firstElem ~ (op ~ elem).rep

    p.flatMap {
      case (first, rest) =>
        rest.foldLeft(Pass(first)) { case (a, (f, e)) => a.flatMap(x => f(x, e)) }
    }
  }

  private def plus[_: P]: P[(NamedExpr, NamedExpr) => P[NamedExpr]] =
    numExpr(
      P("+"),
      new Bind3R[Expression.Aux, Expression.Aux, Numeric, Expression.Aux] {
        override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], n: Numeric[T]): Expression.Aux[T] =
          PlusExpr(x, y)(n)
      },
      infixNamer("plus")
    )

  private def minus[_: P]: P[(NamedExpr, NamedExpr) => P[NamedExpr]] =
    numExpr(
      P("-"),
      new Bind3R[Expression.Aux, Expression.Aux, Numeric, Expression.Aux] {
        override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], n: Numeric[T]): Expression.Aux[T] =
          MinusExpr(x, y)(n)
      },
      infixNamer("minus")
    )

  private def multiply[_: P]: P[(NamedExpr, NamedExpr) => P[NamedExpr]] =
    numExpr(
      P("*"),
      new Bind3R[Expression.Aux, Expression.Aux, Numeric, Expression.Aux] {
        override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], n: Numeric[T]): Expression.Aux[T] =
          TimesExpr(x, y)(n)
      },
      infixNamer("mul")
    )

  private def infixNamer(s: String): (String, String) => String = (a, b) => s"${a}_${s}_${b}"
  private def fNamer(f: String): String => String = a => s"${f}_${a}"

  private def divide[_: P]: P[(NamedExpr, NamedExpr) => P[NamedExpr]] =
    P("/").map(_ =>
      (a: NamedExpr, b: NamedExpr) =>
        ExprPair.alignTypes(a._1, b._1) match {
          case Right(pair) if pair.dataType.integral.isDefined =>
            Pass(DivIntExpr(pair.a, pair.b)(pair.dataType.integral.get) -> infixNamer("div")(a._2, b._2))
          case Right(pair) if pair.dataType.fractional.isDefined =>
            Pass(DivFracExpr(pair.a, pair.b)(pair.dataType.fractional.get) -> infixNamer("div")(a._2, b._2))
          case Right(pair) => Fail(s"Cannot apply math operations to ${pair.dataType}")
          case Left(msg)   => Fail(msg)
        }
    )

  def namedExpr[_: P](r: Recognizer): P[NamedExpr] =
    chained1(minusMathTerm(r) | mathTerm(r), mathTerm(r), plus | minus)

  def expr[_: P](r: Recognizer): P[Expression] = namedExpr(r).map(_._1)

//  def fullExpr[_: P](r: Recognizer): P[Expression] = P(expr(r) ~ End)

  def minusMathTerm[_: P](r: Recognizer): P[NamedExpr] = P("-" ~ mathTerm(r)).flatMap {
    case (x, name) =>
      x.dataType.numeric match {
        case Some(n) => Pass(UnaryMinusExpr(x.aux)(n, x.dataType) -> name)
        case None    => Fail(s"Unary minus cannot be applied to $x of type ${x.dataType}")
      }
  }

  def mathTerm[_: P](r: Recognizer): P[NamedExpr] = chained(mathFactor(r), multiply | divide)

  def mathFactor[_: P](r: Recognizer): P[NamedExpr] =
    P(functionCallExpr(r) | /* caseExpr | */ constExpr | fieldNameExpr(r) | "(" ~ namedExpr(r) ~ ")")

  def fieldNameExpr[_: P](r: Recognizer): P[NamedExpr] =
    fieldWithSchema.flatMap(x => r(x).right.map(_ -> x).fold(Fail(_), Pass(_)))

  def field[_: P](r: Recognizer): P[QueryField] = P(namedExpr(r) ~ alias.? ~ End).map {
    case (e, n, a) => QueryField(a.getOrElse(n), e)
  }

  trait Bind[A[_], Z] {
    def apply[T](x: A[T]): Z
  }

  trait Bind2[A[_], B[_], Z] {
    def apply[T](x: A[T], y: B[T]): Z
  }

  trait Bind3[A[_], B[_], C[_], Z] {
    def apply[T](x: A[T], y: B[T], z: C[T]): Z
  }

  trait Bind3R[A[_], B[_], C[_], Z[_]] {
    def apply[T](x: A[T], y: B[T], z: C[T]): Z[T]
  }

  def op[_: P]: P[(NamedExpr, NamedExpr) => P[(Expression.Condition, String)]] = P(
    P("=").map(_ =>
      binaryExpr(
        new Bind2[Expression.Aux, Expression.Aux, Expression.Condition] {
          override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T]): Condition =
            EqExpr(x, y)
        },
        infixNamer("eq")
      )
    ) |
      P("<>" | "!=").map(_ =>
        binaryExpr(
          new Bind2[Expression.Aux, Expression.Aux, Expression.Condition] {
            override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T]): Condition =
              NeqExpr(x, y)
          },
          infixNamer("ne")
        )
      ) |
      cmpExpr(
        P(">="),
        new Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition] {
          override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], z: Ordering[T]): Condition =
            GeExpr(x, y)(z)
        },
        infixNamer("ge")
      ) |
      cmpExpr(
        P(">"),
        new Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition] {
          override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], z: Ordering[T]): Condition =
            GtExpr(x, y)(z)
        },
        infixNamer("gt")
      ) |
      cmpExpr(
        P("<="),
        new Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition] {
          override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], z: Ordering[T]): Condition =
            LeExpr(x, y)(z)
        },
        infixNamer("le")
      ) |
      cmpExpr(
        P("<"),
        new Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition] {
          override def apply[T](x: Expression.Aux[T], y: Expression.Aux[T], z: Ordering[T]): Condition =
            LtExpr(x, y)(z)
        },
        infixNamer("lt")
      )
  )

  private def numExpr[_: P](
      p: => P[Unit],
      cTor: Bind3R[Expression.Aux, Expression.Aux, Numeric, Expression.Aux],
      namer: (String, String) => String
  ): P[(NamedExpr, NamedExpr) => P[NamedExpr]] = {
    p.map(_ =>
      (a: NamedExpr, b: NamedExpr) =>
        ExprPair.alignTypes(a._1, b._1) match {
          case Right(pair) if pair.dataType.numeric.isDefined =>
            Pass(cTor(pair.a, pair.b, pair.dataType.numeric.get) -> namer(a._2, b._2))
          case Right(pair) => Fail(s"Cannot apply math operations to ${pair.dataType}")
          case Left(msg)   => Fail(msg)
        }
    )
  }

  private def cmpExpr[_: P](
      p: => P[Unit],
      cTor: Bind3[Expression.Aux, Expression.Aux, Ordering, Expression.Condition],
      namer: (String, String) => String
  ): P[(NamedExpr, NamedExpr) => P[(Expression.Condition, String)]] = {
    p.map(_ =>
      (a: NamedExpr, b: NamedExpr) =>
        ExprPair.alignTypes(a._1, b._1) match {
          case Right(pair) if pair.dataType.ordering.isDefined =>
            Pass(cTor(pair.a, pair.b, pair.dataType.ordering.get) -> namer(a._2, b._2))
          case Right(_)  => Fail(s"Cannot compare types ${a._1.dataType} and ${b._1.dataType}")
          case Left(msg) => Fail(msg)
        }
    )
  }

  private def binaryExpr[O, _: P](
      cTor: Bind2[Expression.Aux, Expression.Aux, Expression.Aux[O]],
      namer: (String, String) => String
  ): (NamedExpr, NamedExpr) => P[(Expression.Aux[O], String)] = { (a: NamedExpr, b: NamedExpr) =>
    ExprPair.alignTypes(a._1, b._1) match {
      case Right(pair) if pair.dataType.ordering.isDefined => Pass(cTor(pair.a, pair.b) -> namer(a._2, b._2))
      case Left(msg)                                       => Fail(msg)
    }
  }

  def functionCallExpr[_: P](r: Recognizer): P[NamedExpr] = P(
    unary(r, "trunkYear", DataType[Time], TruncYearExpr.apply) |
      unary(r, "trunkMonth", DataType[Time], TruncMonthExpr.apply) |
      unary(r, "trunkDay", DataType[Time], TruncDayExpr.apply) |
      unary(r, "trunkHour", DataType[Time], TruncHourExpr.apply) |
      unary(r, "trunkMinute", DataType[Time], TruncMinuteExpr.apply) |
      unary(r, "trunkSecond", DataType[Time], TruncSecondExpr.apply) |
      unary(r, "year", DataType[Time], TruncYearExpr.apply) |
      unary(r, "month", DataType[Time], TruncMonthExpr.apply) |
      unary(r, "day", DataType[Time], TruncDayExpr.apply) |
      unary(r, "hour", DataType[Time], TruncHourExpr.apply) |
      unary(r, "minute", DataType[Time], TruncMinuteExpr.apply) |
      unary(r, "second", DataType[Time], TruncSecondExpr.apply) |
      aggregateOrd(r, "min", new Bind2[Expression.Aux, Ordering, Expression] {
        override def apply[T](x: Expression.Aux[T], o: Ordering[T]): Expression = MinExpr(x)(o)
      }) |
      aggregateOrd(r, "max", new Bind2[Expression.Aux, Ordering, Expression] {
        override def apply[T](x: Expression.Aux[T], o: Ordering[T]): Expression = MaxExpr(x)(o)
      }) |
      aggregateNum(r, "sum", new Bind2[Expression.Aux, Numeric, Expression] {
        override def apply[T](x: Expression.Aux[T], o: Numeric[T]): Expression = MaxExpr(x)(o)
      }) |
      aggregate(r, "count", e => CountExpr(e.aux)) |
      aggregate(r, "distinct_count", e => DistinctCountExpr(e.aux)) |
      aggregate(r, "distinct_random", e => DistinctRandomExpr(e.aux))
  )

  def callOrField[_: P](r: Recognizer): P[Expression] = P(functionCallExpr(r) | fieldNameExpr(r)).map(_._1)

  def comparison[_: P](r: Recognizer): P[Expression.Condition] =
    P(expr(r) ~ op ~/ expr(r)).flatMap {
      case (a, o, b) => o(a -> "aaa", b -> "bbb").map(_._1)
    }

  def in[_: P](r: Recognizer): P[Expression.Condition] = {
    def p = P(expr(r) ~ inWord ~/ "(" ~ constExpr.rep(min = 1, sep = ",") ~ ")")

    p.flatMap {
      case (e, vs) =>
        convertConstants(vs.map(_._1), e.dataType.aux) match {
          case Right(values) => Pass(InExpr(e.aux, values.toSet))
          case Left(msg)     => Fail(msg)
        }
    }
  }

  def notIn[_: P](r: Recognizer): P[Expression.Condition] = {
    def p = P(expr(r) ~ notWord ~ inWord ~/ "(" ~ constExpr.rep(min = 1, sep = ",") ~ ")")

    p.flatMap {
      case (e, vs) =>
        convertConstants(vs.map(_._1), e.dataType.aux) match {
          case Right(values) => Pass(NotInExpr(e.aux, values.toSet))
          case Left(msg)     => Fail(msg)
        }
    }
  }

  def isNull[_: P](r: Recognizer): P[Expression.Condition] =
    P(expr(r) ~ isWord ~ nullWord).map(e => IsNullExpr(e.aux))

  def isNotNull[_: P](r: Recognizer): P[Expression.Condition] =
    P(expr(r) ~ isWord ~ notWord ~ nullWord).map(e => IsNotNullExpr(e.aux))

  def boolExpr[_: P](r: Recognizer): P[Expression.Condition] =
    P(comparison(r) | in(r) | notIn(r) | isNull(r) | isNotNull(r) | expr(r)).flatMap { e =>
      if (e.dataType == DataType[Boolean]) Pass(e.asInstanceOf[Expression.Condition])
      else Fail("Expression shall have type BOOL")
    }

  def condition[_: P](r: Recognizer): P[Expression.Condition] = P(logicalTerm(r) ~ (orWord ~/ logicalTerm(r)).rep).map {
    case (x, y) if y.nonEmpty => OrExpr(x +: y)
    case (x, _)               => x
  }

  def logicalTerm[_: P](r: Recognizer): P[Expression.Condition] =
    P(logicalFactor(r) ~ (andWord ~/ logicalFactor(r)).rep).map {
      case (x, y) if y.nonEmpty => AndExpr(x +: y)
      case (x, _)               => x
    }

  def logicalFactor[_: P](r: Recognizer): P[Expression.Condition] = P(boolExpr(r) | ("(" ~ condition(r) ~ ")"))

  def limit[_: P]: P[Int] = P(limitWord ~/ NewValueParser.intNumber)

  def where[_: P](r: Recognizer): P[Expression.Condition] = P(whereWord ~/ condition(r))

  def grouping[_: P](r: Recognizer): P[Expression] = callOrField(r)

  def groupings[_: P](r: Recognizer): P[Seq[Expression]] = P(groupWord ~/ byWord ~ grouping(r).rep(1, ","))

  def having[_: P](r: Recognizer): P[Expression.Condition] = P(havingWord ~/ condition(r))

  /* SELECT */

  def fieldList[_: P](r: Recognizer): P[SqlFieldList] =
    P(field(r).rep(min = 1, sep = ",")).map(SqlFieldList)
  def allFields[_: P]: P[SqlFieldsAll.type] = P(asterisk).map(_ => SqlFieldsAll)

  def selectFields[_: P](r: Recognizer): P[SqlFields] =
    P(selectWord ~/ (fieldList(r) | allFields))

  /*def nestedSelectFrom[_: P](fields: SqlFields): P[Query] = {
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
  }*/

  def table[_: P]: P[Table] = P(tableName).flatMap(tableByName)

  def normalSelectFrom[_: P](r: Recognizer, fields: SqlFields): P[Query] = {
    fields match {
      case SqlFieldsAll => Fail("* is not supported")
      case SqlFieldList(fs) =>
        P((fromWord ~ table).? ~/ where(r).? ~ groupings(r).? ~ having(r).? ~ limit.?).map {
          case (t, c, gs, h, l) => Query(t, fs, c, gs.getOrElse(Seq.empty), l, h)
        }
    }
  }

  def selectFrom[_: P](r: Recognizer)(fields: SqlFields): P[Query] = {
    P( /*nestedSelectFrom(fields) |*/ normalSelectFrom(r, fields))
  }

  def select[_: P](r: Recognizer): P[Query] = P(selectFields(r).flatMap(selectFrom(r)))

  /* SHOW */
  def tables[_: P]: P[ShowTables.type] = P(tablesWord).map(_ => ShowTables)

  def columns[_: P]: P[ShowColumns] = P(columnsWord ~/ fromWord ~ tableName).flatMap(tableByName).map(ShowColumns)

  def queries[_: P]: P[ShowQueryMetrics] =
    P(queriesWord ~/ queryMetricsFilter.? ~/ limit.?).map(ShowQueryMetrics.tupled)

  def show[_: P]: P[Statement] = P(showWord ~/ (columns | tables | queries))

  /* KILL */
  def metricQueryIdFilter[_: P]: P[MetricsFilter] =
    P(queryIdWord ~ "=" ~/ NewValueParser.string).map(queryId => MetricsFilter(queryId = Some(queryId)))
  def metricStateFilter[_: P]: P[MetricsFilter] =
    P(stateWord ~ "=" ~/ NewValueParser.string).map(state => MetricsFilter(state = Some(state)))
  def queryMetricsFilter[_: P]: P[MetricsFilter] = P(
    whereWord ~ (metricQueryIdFilter | metricStateFilter)
  )

  def kill[_: P]: P[Statement] = P(killWord ~/ queryWord ~/ whereWord ~ metricQueryIdFilter).map(KillQuery)

  /* DELETE */
  def delete[_: P]: P[DeleteQueryMetrics] =
    P(deleteWord ~/ queriesWord ~/ queryMetricsFilter).map(DeleteQueryMetrics)

  /* UPSERT */
  def upsertFields[_: P]: P[Seq[String]] = "(" ~/ fieldWithSchema.rep(min = 1, sep = ",") ~ ")"

  def values[_: P](count: Int): P[Seq[Seq[Expression]]] =
    ("(" ~/ expr(noField).rep(exactly = count, sep = ",") ~ ")").opaque(s"<$count expressions>").rep(1, ",")

  def upsert[_: P]: P[Upsert] =
    for {
      fv <- P(upsertWord ~ intoWord ~/ tableName ~/ upsertFields ~ valuesWord)
      (tableName, fields) = fv
      vs <- values(fields.size)
      table <- tableByName(tableName)
    } yield {
      Upsert(table, fields, vs)
    }

  def statement[_: P](r: Recognizer): P[Statement] = P((select(r) | upsert | show | kill | delete) ~ ";".? ~ End)

  private def unary[_: P](r: Recognizer, name: String): P[NamedExpr] = P(IgnoreCase(name) ~ "(" ~ namedExpr(r) ~ ")")

  private def unary[T, _: P](
      r: Recognizer,
      fn: String,
      tpe: DataType.Aux[T],
      create: Expression.Aux[T] => Expression
  ): P[NamedExpr] = {
    unary(r, fn).flatMap {
      case (NullExpr(t), n) => Pass(NullExpr(t) -> n)
      case (e, n) =>
        if (e.dataType == tpe)
          Pass(create(e.asInstanceOf[Expression.Aux[T]]) -> fNamer(fn)(n))
        else Fail(s"Function $fn can not be applied to ${e.dataType}")
    }
  }

  private def aggregate[_: P](
      r: Recognizer,
      fn: String,
      create: Expression => Expression
  ): P[NamedExpr] = {
    unary(r, fn).map {
      case (e, n) => create(e) -> fNamer(fn)(n)
    }
  }

  private def aggregateOrd[_: P](
      r: Recognizer,
      fn: String,
      create: Bind2[Expression.Aux, Ordering, Expression]
  ): P[NamedExpr] = {
    unary(r, fn).flatMap {
      case (NullExpr(t), n) => Pass(NullExpr(t) -> n)
      case (e, n) =>
        e.dataType.ordering match {
          case Some(o) => Pass(create(e.aux, o) -> fNamer(fn)(n))
          case None    => Fail(s"Cannot apply $fn to ${e.dataType}")
        }
    }
  }

  private def aggregateNum[_: P](
      r: Recognizer,
      fn: String,
      create: Bind2[Expression.Aux, Numeric, Expression]
  ): P[NamedExpr] = {
    unary(r, fn).flatMap {
      case (NullExpr(t), n) => Pass(NullExpr(t) -> n)
      case (e, n) =>
        e.dataType.numeric match {
          case Some(num) => Pass(create(e.aux, num) -> fNamer(fn)(n))
          case None      => Fail(s"Cannot apply $fn to ${e.dataType}")
        }
    }
  }

  private def tableByName[_: P](tableName: String): P[Table] = {
    optionToP(schema.getTable(tableName), s"Unknown table '$tableName'")
  }

  private def noField[_: P](name: String): Either[String, Expression] = Left("Table not defined, so no fields allowed")

  def nullField(name: String): Either[String, Expression] = Right(NullExpr(DataType[Null]))

  def fieldByName(table: Table)(name: String): Either[String, Expression] = {
    getFieldByName(table)(name).toRight(s"Unknown field $name")
  }

  private def fieldOrAlias[_: P](table: Table, fields: Seq[QueryField])(name: String): Either[String, Expression] = {
    fields
      .find(_.name == name)
      .map(_.expr)
      .orElse(getFieldByName(table)(name))
      .toRight(s"Unknown field $name")
  }

  private def getFieldByName(table: Table)(name: String): Option[Expression] = {
    val lowerName = name.toLowerCase
    if (lowerName == Table.TIME_FIELD_NAME) {
      Some(TimeExpr)
    } else {
      getMetricExpr(table, lowerName) orElse getDimExpr(table, lowerName) orElse getLinkExpr(table, name)
    }
  }

  private def getMetricExpr(table: Table, fieldName: String): Option[MetricExpr[_]] = {
    table.metrics.find(_.name.toLowerCase == fieldName).map(f => MetricExpr(f.aux))
  }

  private def getDimExpr(table: Table, fieldName: String): Option[DimensionExpr[_]] = {
    table.dimensionSeq.find(_.name.toLowerCase == fieldName).map(d => DimensionExpr(d.aux))
  }

  private def getLinkExpr(table: Table, fieldName: String): Option[LinkExpr[_]] = {

    val pos = fieldName.indexOf('_')

    if (pos > 0) {
      val catName = fieldName.substring(0, pos)
      val catField = fieldName.substring(pos + 1)
      for {
        c <- table.externalLinks.find(_.linkName equalsIgnoreCase catName)
        f <- c.fields.find { m => m.name equalsIgnoreCase catField }
      } yield new LinkExpr(c, f.aux)
    } else {
      None
    }
  }

  private def convertConstants(constants: Seq[ConstantExpr], dataType: DataType): Either[String, Seq[dataType.T]] = {
    CollectionUtils.collectErrors(constants.map(c => ExprPair.constCast(c, dataType)))
  }

  private def optionToP[_: P, T](v: Option[T], msg: String): P[T] = {
    v match {
      case Some(t) => Pass(t)
      case None    => Fail(msg)
    }
  }

  private def formatFailure(sql: String, f: Parsed.Failure): String = {
    val trace = f.trace()
    val expected = trace.terminals.render
    val actual = Util.literalize(f.extra.input.slice(f.index, f.index + 10))
    val position = f.extra.input.prettyIndex(f.index)

    s"""Invalid SQL statement: '$sql'
        |Expect $expected, but got $actual at $position""".stripMargin

  }

  def parse(sql: String): Either[String, Statement] = {
    fastparse.parse(sql, statement(nullField)(_)) match {
      case Parsed.Success(Query(Some(table), _, _, _, _, _), _) =>
        fastparse.parse(sql, statement(fieldByName(table))(_)) match {
          case Parsed.Success(statement, _) => Right(statement)
          case f: Parsed.Failure            => Left(formatFailure(sql, f))
        }
      case f: Parsed.Failure => Left(formatFailure(sql, f))
    }

  }

//  private def eitherToP[_: P, T](v: Either[String, T]): P[T] = {
//    v match {
//      case Right(t)  => Pass(t)
//      case Left(msg) => Fail(msg)
//    }
//  }
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
