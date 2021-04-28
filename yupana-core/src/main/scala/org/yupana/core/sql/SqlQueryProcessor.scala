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

package org.yupana.core.sql

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, MetricValue, Schema, Table }
import org.yupana.api.types._
import org.yupana.api.utils.CollectionUtils
import org.yupana.core.RuntimeCalculator
import org.yupana.core.sql.SqlQueryProcessor.ExprType.ExprType
import org.yupana.core.sql.parser.{ SqlFieldList, SqlFieldsAll }

class SqlQueryProcessor(schema: Schema) extends QueryValidator {

  import SqlQueryProcessor._

  private val expressionCalculator = new RuntimeCalculator(schema.tokenizer)

  def createQuery(select: parser.Select, parameters: Map[Int, parser.Value] = Map.empty): Either[String, Query] = {
    val state = new BuilderState(parameters)
    val query = for {
      table <- getTable(select.schemaName).right
      fields <- getFields(table, select, state).right
      filter <- getFilter(table, fields, select.condition, state).right
      groupBy <- getGroupBy(select, table, state).right
      pf <- getPostFilter(table, fields, select.having, state).right
    } yield {
      Query(table, fields, filter, groupBy, select.limit, pf)
    }

    query.right.flatMap(validateQuery)
  }

  def createDataPoints(
      upsert: parser.Upsert,
      parameters: Seq[Map[Int, parser.Value]]
  ): Either[String, Seq[DataPoint]] = {
    val params = if (parameters.isEmpty) Seq(Map.empty[Int, parser.Value]) else parameters

    if (upsert.values.forall(_.size == upsert.fieldNames.size)) {
      (for {
        mayBeTable <- getTable(Some(upsert.schemaName)).right
        table <- mayBeTable.toRight("Table is not defined").right
        fieldMap <- getFieldMap(table, upsert.fieldNames).right
      } yield (table, fieldMap)).right.flatMap {
        case (table, fieldMap) =>
          val dps = params.flatMap { ps =>
            val state = new BuilderState(ps)

            upsert.values.map { values =>
              for {
                values <- getValues(state, table, values).right
                time <- getTimeValue(fieldMap, values).right
                dimensions <- getDimensionValues(table, fieldMap, values).right
                metrics <- getMetricValues(table, fieldMap, values).right
              } yield {
                DataPoint(table, time, dimensions, metrics)
              }
            }
          }
          CollectionUtils.collectErrors(dps)
      }
    } else {
      Left("Inconsistent UPSERT")
    }
  }

  private def getTable(schemaName: Option[String]): Either[String, Option[Table]] = {
    schemaName match {
      case Some(name) =>
        schema.getTable(name).map(Some(_)).toRight(s"Unknown table '$name'")
      case None =>
        Right(None)
    }
  }

  val function0Registry: Map[String, BuilderState => Expression[_]] = Map(
    "now" -> ((s: BuilderState) => ConstantExpr(Time(s.queryStartTime)))
  )

  private def getFields(
      table: Option[Table],
      select: parser.Select,
      state: BuilderState
  ): Either[String, Seq[QueryField]] = {

    select.fields match {
      case SqlFieldList(fs) =>
        val fields = fs.map(f => getField(table, f, state))
        CollectionUtils.collectErrors(fields)

      case SqlFieldsAll =>
        Left("All fields matching is not supported")
    }
  }

  private def getField(
      table: Option[Table],
      field: parser.SqlField,
      state: BuilderState
  ): Either[String, QueryField] = {
    val fieldName = state.fieldName(field)

    val resolver = table.map(fieldByName).getOrElse(constOnly)

    createExpr(state, resolver, field.expr, ExprType.Math).right.map(_.as(fieldName))
  }

  private def fieldByRef(table: Table, fields: Seq[QueryField])(name: String): Option[Expression[_]] = {
    fields.find(_.name == name).map(f => f.expr).orElse(fieldByName(table)(name))
  }

  private def constOrRef(fields: Seq[QueryField])(name: String): Option[Expression[_]] = {
    fields.find(_.name == name).map(f => f.expr).orElse(constOnly(name))
  }

  private def createExpr(
      state: BuilderState,
      nameResolver: NameResolver,
      expr: parser.SqlExpr,
      exprType: ExprType
  ): Either[String, Expression[_]] = {
    val e = expr match {
      case parser.Case(cs, default) =>
        val converted = CollectionUtils.collectErrors(cs.map {
          case (condition, value) =>
            for {
              et <- createExpr(state, nameResolver, value, exprType).right
              c <- createCondition(state, nameResolver, condition.simplify).right
            } yield (c, et)
        })

        createExpr(state, nameResolver, default, exprType).right.flatMap(ve =>
          converted.right.flatMap { conv =>
            conv.foldRight(Right(ve): Either[String, Expression[_]]) {
              case ((condition, value), Right(e)) =>
                ExprPair
                  .alignTypes(value, e)
                  .right
                  .map(pair => ConditionExpr(condition, pair.a, pair.b).asInstanceOf[Expression[_]])

              case (_, Left(msg)) => Left(msg)
            }
          }
        )

      case parser.FieldName(name) =>
        nameResolver(name) toRight s"Unknown field $name"

      case parser.Constant(v) => convertValue(state, v, exprType)

      case parser.SqlArray(vs) =>
        val consts = CollectionUtils.collectErrors(vs.map(v => convertValue(state, v, exprType)))
        consts.right.flatMap(createArrayExpr)

      case parser.UMinus(a) =>
        createUMinus(state, nameResolver, a, ExprType.Math)

      case parser.Plus(l, r) =>
        createBinary(state, nameResolver, l, r, "+", ExprType.Math)

      case parser.Minus(l, r) =>
        createBinary(state, nameResolver, l, r, "-", ExprType.Math)

      case parser.Multiply(l, r) =>
        createBinary(state, nameResolver, l, r, "*", ExprType.Math)

      case parser.Divide(l, r) =>
        createBinary(state, nameResolver, l, r, "/", ExprType.Math)

      case parser.FunctionCall(f, Nil) =>
        function0Registry.get(f).map(_(state)).toRight(s"Unknown nullary function $f")

      case parser.FunctionCall(f, e :: Nil) =>
        for {
          ex <- createExpr(state, nameResolver, e, exprType).right
          fexpr <- FunctionRegistry.unary(f, ex).right
        } yield fexpr

      case parser.FunctionCall(f, e1 :: e2 :: Nil) =>
        for {
          a <- createExpr(state, nameResolver, e1, exprType).right
          b <- createExpr(state, nameResolver, e2, exprType).right
          fexpr <- FunctionRegistry.bi(f, a, b).right
        } yield fexpr

      case parser.FunctionCall(f, _) =>
        Left(s"Undefined function $f")
    }

    e.right.map {
      case die: DimensionIdExpr => die
      case ex if exprType == ExprType.Cmp && ex.dataType == DataType[String] && ex.kind != Const =>
        LowerExpr(ex.asInstanceOf[Expression[String]])
      case ex => ex
    }
  }

  private def createUMinus(
      state: SqlQueryProcessor.BuilderState,
      resolver: NameResolver,
      expr: parser.SqlExpr,
      exprType: ExprType
  ): Either[String, Expression[_]] = {
    expr match {
      // TODO: this may be removed when we will calculate constant values before query execution
      case parser.Constant(parser.NumericValue(n)) => Right(ConstantExpr(-n))
      case _ =>
        for {
          e <- createExpr(state, resolver, expr, exprType).right
          u <- FunctionRegistry.unary("-", e).right
        } yield u
    }
  }

  private def createArrayExpr(expressions: Seq[ConstantExpr[_]]): Either[String, Expression[_]] = {
    // we assume all expressions have exact same type, but it might require to align type in future
    val first = expressions.head

    val incorrectType = expressions.collect {
      case x if x.dataType != first.dataType => x
    }

    if (incorrectType.isEmpty) {
      Right(
        ArrayExpr[first.dataType.T](expressions.toArray.asInstanceOf[Array[Expression[first.dataType.T]]])(
          first.dataType
        )
      )
    } else {
      val err = incorrectType.map(e => s"$e has type ${e.dataType}").mkString(", ")
      Left(s"All expressions must have same type but: $err}")
    }
  }

  private def createBinary(
      state: BuilderState,
      nameResolver: NameResolver,
      l: parser.SqlExpr,
      r: parser.SqlExpr,
      fun: String,
      exprType: ExprType
  ): Either[String, Expression[_]] =
    for {
      le <- createExpr(state, nameResolver, l, exprType).right
      re <- createExpr(state, nameResolver, r, exprType).right
      biFunction <- FunctionRegistry.bi(fun, le, re).right
    } yield biFunction

  def createBooleanExpr(l: Expression[_], r: Expression[_], fun: String): Either[String, Expression[Boolean]] = {
    FunctionRegistry.bi(fun, l, r).right.flatMap { e =>
      if (e.dataType == DataType[Boolean]) Right(e.asInstanceOf[Expression[Boolean]])
      else Left(s"$fun result type is ${e.dataType.meta.sqlType} but BOOLEAN required")
    }
  }

  private def createCondition(
      state: BuilderState,
      nameResolver: NameResolver,
      c: parser.Condition
  ): Either[String, Condition] = {

    def construct(cmpName: String, a: parser.SqlExpr, b: parser.SqlExpr): Either[String, Condition] = {
      for {
        l <- createExpr(state, nameResolver, a, ExprType.Cmp).right
        r <- createExpr(state, nameResolver, b, ExprType.Cmp).right
        op <- createBooleanExpr(l, r, cmpName).right
      } yield op
    }

    c match {
      case parser.Eq(e, v) => construct("=", e, v)
      case parser.Ne(e, v) => construct("<>", e, v)
      case parser.Lt(e, v) => construct("<", e, v)
      case parser.Gt(e, v) => construct(">", e, v)
      case parser.Le(e, v) => construct("<=", e, v)
      case parser.Ge(e, v) => construct(">=", e, v)

      case parser.IsNull(e) =>
        for {
          ne <- createExpr(state, nameResolver, e, ExprType.Math).right
        } yield IsNullExpr(ne)

      case parser.IsNotNull(e) =>
        for {
          nne <- createExpr(state, nameResolver, e, ExprType.Math).right
        } yield IsNotNullExpr(nne)

      case parser.In(e, vs) =>
        createExpr(state, nameResolver, e, ExprType.Cmp).right.flatMap {
          case ce: Expression[t] =>
            CollectionUtils
              .collectErrors(vs.map(v => convertValue[t](state, v, ce.dataType)))
              .right
              .map(cvs => InExpr(ce, cvs.toSet))
        }

      case parser.NotIn(e, vs) =>
        createExpr(state, nameResolver, e, ExprType.Cmp).right.flatMap {
          case ce: Expression[t] =>
            CollectionUtils
              .collectErrors(vs.map(v => convertValue[t](state, v, ce.dataType)))
              .right
              .map(cvs => NotInExpr(ce, cvs.toSet))
        }
      case parser.And(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(state, nameResolver, c))).right.map(AndExpr)

      case parser.Or(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(state, nameResolver, c))).right.map(OrExpr)

      case parser.ExprCondition(e) =>
        createExpr(state, nameResolver, e, ExprType.Cmp).right.flatMap { ex =>
          if (ex.dataType == DataType[Boolean]) {
            Right(ex.asInstanceOf[Expression[Boolean]])
          } else {
            Left(s"$ex has type ${ex.dataType}, but BOOLEAN is required")
          }
        }

      case parser.BetweenCondition(e, f, t) =>
        createExpr(state, nameResolver, e, ExprType.Cmp).right.flatMap {
          case ex: Expression[t] =>
            for {
              from <- convertValue[t](state, f, ex.dataType).right
              to <- convertValue[t](state, t, ex.dataType).right
              ge <- createBooleanExpr(ex, ConstantExpr(from)(ex.dataType), ">=").right
              le <- createBooleanExpr(ex, ConstantExpr(to)(ex.dataType), "<=").right
            } yield AndExpr(Seq(ge, le))
        }
    }
  }

  private def convertValue[T](state: BuilderState, v: parser.Value, dataType: DataType.Aux[T]): Either[String, T] = {
    convertValue(state, v, ExprType.Cmp).right.flatMap(const => ExprPair.constCast(const, dataType))
  }

  private def convertValue(
      state: BuilderState,
      v: parser.Value,
      exprType: ExprType
  ): Either[String, ConstantExpr[_]] = {
    v match {
      case parser.StringValue(s) =>
        val const = if (exprType == ExprType.Cmp) s.toLowerCase else s
        Right(ConstantExpr(const))

      case parser.NumericValue(n) =>
        Right(ConstantExpr(n))

      case parser.TimestampValue(t) =>
        Right(ConstantExpr(Time(t)))

      case parser.PeriodValue(p) if exprType == ExprType.Cmp =>
        if (p.getYears == 0 && p.getMonths == 0) {
          Right(ConstantExpr(p.toStandardDuration.getMillis))
        } else {
          Left(s"Period $p cannot be used as duration, because it has months or years")
        }

      case parser.PeriodValue(p) => Right(ConstantExpr(p))

      case parser.Placeholder =>
        state.nextPlaceholderValue().right.flatMap(v => convertValue(state, v, exprType))
    }
  }

  private def getFilter(
      table: Option[Table],
      fields: Seq[QueryField],
      condition: Option[parser.Condition],
      state: BuilderState
  ): Either[String, Option[Condition]] = {
    val resolver = table.map(t => fieldByRef(t, fields)(_)).getOrElse(constOrRef(fields)(_))
    condition match {
      case Some(c) =>
        createCondition(state, resolver, c.simplify).right.map(Some(_))
      case None => Right(None)
    }
  }

  private def getPostFilter(
      table: Option[Table],
      fields: Seq[QueryField],
      condition: Option[parser.Condition],
      state: BuilderState
  ): Either[String, Option[Condition]] = {
    val resolver = table.map(t => fieldByRef(t, fields)(_)).getOrElse(constOrRef(fields)(_))
    condition match {
      case Some(c) =>
        createCondition(state, resolver, c.simplify).right.map(Some(_))
      case None => Right(None)
    }
  }

  private def substituteGroupings(select: parser.Select): Seq[parser.SqlExpr] = {
    select.groupings.map {
      case g @ parser.FieldName(n) =>
        select.fields match {
          case SqlFieldList(fields) => fields.find(_.alias.contains(n)).map(_.expr).getOrElse(g)
          case SqlFieldsAll         => g
        }
      case x => x
    }
  }

  private def getGroupBy(
      select: parser.Select,
      table: Option[Table],
      state: BuilderState
  ): Either[String, Seq[Expression[_]]] = {
    val filled = substituteGroupings(select)
    val resolver = table.map(fieldByName).getOrElse(constOnly)

    val groupBy = filled.map { sqlExpr =>
      createExpr(state, resolver, sqlExpr, ExprType.Math)
    }

    CollectionUtils.collectErrors(groupBy)
  }

  private val constOnly: String => Option[Expression[_]] = _ => None

  private def fieldByName(table: Table)(name: String): Option[Expression[_]] = {
    val lowerName = name.toLowerCase
    if (lowerName == TIME_FIELD) {
      Some(TimeExpr)
    } else {
      getMetricExpr(table, lowerName) orElse getDimExpr(table, lowerName) orElse getLinkExpr(table, name)
    }
  }

  private def getMetricExpr(table: Table, fieldName: String): Option[Expression[_]] = {
    table.metrics.find(_.name.toLowerCase == fieldName).map(f => MetricExpr(f.aux))
  }

  private def getDimExpr(table: Table, fieldName: String): Option[Expression[_]] = {
    table.dimensionSeq.find(_.name.toLowerCase == fieldName).map(d => DimensionExpr(d.aux))
  }

  private def getLinkExpr(table: Table, fieldName: String): Option[Expression[_]] = {

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

  private def getFieldMap(table: Table, fieldNames: Seq[String]): Either[String, Map[Expression[_], Int]] = {
    val exprs = CollectionUtils.collectErrors[Expression[_]](
      fieldNames.map { name =>
        fieldByName(table)(name) match {
          case Some(LinkExpr(_, _)) => Left(s"External link field $name cannot be upserted")
          case Some(x)              => Right(x)
          case None                 => Left(s"Unknown field $name")
        }
      }
    )
    exprs.right.map(_.zipWithIndex.toMap)
  }

  private def getValues(
      state: BuilderState,
      table: Table,
      values: Seq[parser.SqlExpr]
  ): Either[String, Array[ConstantExpr[_]]] = {
    val vs = values.map { v =>
      createExpr(state, fieldByName(table), v, ExprType.Math) match {
        case Right(e: Expression[t]) if e.kind == Const =>
          val eval = expressionCalculator.evaluateConstant[t](e)
          if (eval != null) {
            Right(ConstantExpr(eval)(e.dataType.aux).asInstanceOf[ConstantExpr[_]])
          } else {
            Left(s"Cannon evaluate $e")
          }
        case Right(e) => Left(s"$e is not constant")

        case Left(m) => Left(m)
      }
    }

    CollectionUtils.collectErrors[ConstantExpr[_]](vs).right.map(_.toArray)
  }

  private def getTimeValue(fieldMap: Map[Expression[_], Int], values: Array[ConstantExpr[_]]): Either[String, Long] = {
    val idx = fieldMap.get(TimeExpr).toRight("time field is not defined")
    idx.right.map(values).right.flatMap(c => ExprPair.constCast(c, DataType[Time])).right.map(_.millis)
  }

  private def getDimensionValues(
      table: Table,
      fieldMap: Map[Expression[_], Int],
      values: Seq[ConstantExpr[_]]
  ): Either[String, Map[Dimension, _]] = {
    val dimValues = table.dimensionSeq.map { dim =>
      val idx = fieldMap.get(DimensionExpr(dim.aux)).toRight(s"${dim.name} is not defined")
      idx.right.map(values).right.flatMap(c => ExprPair.constCast(c, dim.dataType)).right.map(dim -> _)
    }

    CollectionUtils.collectErrors(dimValues).right.map(_.toMap)
  }

  private def getMetricValues(
      table: Table,
      fieldMap: Map[Expression[_], Int],
      values: Seq[ConstantExpr[_]]
  ): Either[String, Seq[MetricValue]] = {
    val vs = fieldMap.collect {
      case (MetricExpr(m), idx) =>
        ExprPair.constCast(values(idx), m.dataType).right.map(v => MetricValue(m, v))
    }

    CollectionUtils.collectErrors(vs.toSeq)
  }
}

object SqlQueryProcessor {
  type NameResolver = String => Option[Expression[_]]
  val TIME_FIELD: String = Table.TIME_FIELD_NAME

  object ExprType extends Enumeration {
    type ExprType = Value
    val Cmp, Math = Value
  }

  class BuilderState(parameters: Map[Int, parser.Value]) {
    private var fieldNames = Map.empty[String, Int]
    private var nextPlaceholder = 1

    val queryStartTime: LocalDateTime = new LocalDateTime(DateTimeZone.UTC)

    def fieldName(field: parser.SqlField): String = {
      val name = field.alias orElse field.expr.proposedName getOrElse "field"
      fieldNames.get(name) match {
        case Some(i) =>
          fieldNames += name -> (i + 1)
          s"${name}_$i"

        case None =>
          fieldNames += name -> 2
          name
      }
    }

    def nextPlaceholderValue(): Either[String, parser.Value] = {
      val result = parameters.get(nextPlaceholder).toRight(s"Value for placeholder #$nextPlaceholder is not defined")
      nextPlaceholder += 1
      result
    }
  }
}
