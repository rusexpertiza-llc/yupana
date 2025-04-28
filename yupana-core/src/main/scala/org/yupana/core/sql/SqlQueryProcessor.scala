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

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, MetricValue, Schema, Table }
import org.yupana.api.types._
import org.yupana.api.utils.CollectionUtils
import org.yupana.core.ConstantCalculator
import org.yupana.core.sql.SqlQueryProcessor.ExprType.ExprType
import org.yupana.core.sql.parser.{ SqlFieldList, SqlFieldsAll }

import scala.util.Try

class SqlQueryProcessor(schema: Schema) extends QueryValidator with Serializable {

  import SqlQueryProcessor._

  private val calculator = new ConstantCalculator(schema.tokenizer)

  def createQuery(select: parser.Select): Either[String, Query] = {
    val query = for {
      table <- getTable(select.tableName)
      fields <- getFields(table, select)
      filter <- getFilter(table, fields, select.condition)
      groupBy <- getGroupBy(select, table)
      pf <- getPostFilter(table, fields, select.having)
    } yield {
      Query(table, fields, filter, groupBy, select.limit, pf)
    }

    query.flatMap(validateQuery)
  }

  private def getPlaceholders(expr: Expression[_]): List[Expression[_]] = {
    expr.fold(List.empty[Expression[_]]) {
      case (l, p @ PlaceholderExpr(_, _))     => p :: l
      case (l, u @ UntypedPlaceholderExpr(_)) => u :: l
      case (l, _)                             => l
    }
  }

  def bindParameters(query: Query, parameters: Map[Int, Parameter])(
      implicit srw: StringReaderWriter
  ): Either[String, IndexedSeq[Any]] = {
    val allPh = query.fields.flatMap(f => getPlaceholders(f.expr)) ++
      query.filter.toSeq.flatMap(getPlaceholders) ++
      query.groupBy.flatMap(getPlaceholders) ++
      query.postFilter.toSeq.flatMap(getPlaceholders)

    val values = allPh.map {
      case PlaceholderExpr(id, dt) =>
        parameters
          .get(id)
          .toRight(s"Undefined parameter #$id")
          .flatMap {
            case p @ TypedParameter(v) => DataTypeUtils.constCast(v, p.dataType, dt.aux, calculator)
            case UntypedParameter(v)   => Try(dt.storable.readString(v)).toEither.left.map(_.getMessage)
          }
          .map(id -> _)
      case UntypedPlaceholderExpr(id) => Left(s"Cannot deduce parameter #$id type")
      case e                          => Left(s"Unexpected expression $e")
    }

    CollectionUtils.collectErrors(values).map { vs =>
      vs.sortBy(_._1).map(_._2).toIndexedSeq
    }
  }

  def createDataPoints(
      upsert: parser.Upsert,
      parameters: Seq[Map[Int, Parameter]]
  )(implicit srw: StringReaderWriter): Either[String, Seq[DataPoint]] = {
    val params = if (parameters.isEmpty) Seq(Map.empty[Int, Parameter]) else parameters

    if (upsert.values.forall(_.size == upsert.fieldNames.size)) {
      (for {
        mayBeTable <- getTable(Some(upsert.tableName))
        table <- mayBeTable.toRight("Table is not defined")
        fieldMap <- getFieldMap(table, upsert.fieldNames)
      } yield (table, fieldMap)).flatMap {
        case (table, fieldMap) =>
          val dps = params.flatMap { ps =>
            val types = fieldMap.map { case (e, i) => (i, e.dataType) }.toSeq.sortBy(_._1).map(_._2)
            upsert.values.map { values =>
              for {
                consts <- getValues(values zip types, ps)
                time <- getTimeValue(fieldMap, consts)
                dimensions <- getDimensionValues(table, fieldMap, consts)
                metrics <- getMetricValues(fieldMap, consts)
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

  private def getFields(
      table: Option[Table],
      select: parser.Select
  ): Either[String, Seq[QueryField]] = {
    val resolver = table.map(fieldByName).getOrElse(constOnly)

    var fieldNames = Map.empty[String, Int]

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

    select.fields match {
      case SqlFieldList(fs) =>
        val fields = fs.map(f => createExpr(resolver, f.expr, ExprType.Math).map(_.as(fieldName(f))))
        CollectionUtils.collectErrors(fields)

      case SqlFieldsAll =>
        Left("All fields matching is not supported")
    }
  }

  private def fieldByRef(table: Table, fields: Seq[QueryField]): NameResolver = { name =>
    val byRef = fields.find(_.name == name).map(f => f.expr)
    val byName = fieldByName(table)(name)
    (byRef, byName) match {
      case (Some(a), Right(b)) if a != b => Left(s"Ambiguous field $name")
      case (Some(e), _)                  => Right(e)
      case (None, x)                     => x
    }
  }

  private def constOrRef(fields: Seq[QueryField]): NameResolver = { name =>
    fields.find(_.name == name).map(f => f.expr).toRight(s"Unknown field $name")
  }

  private def createExpr(
      nameResolver: NameResolver,
      expr: parser.SqlExpr,
      exprType: ExprType
  ): Either[String, Expression[_]] = {
    val e = expr match {
      case parser.Case(cs, default) =>
        val converted = CollectionUtils.collectErrors(cs.map {
          case (condition, value) =>
            for {
              et <- createExpr(nameResolver, value, exprType)
              c <- createCondition(nameResolver, condition)
            } yield (c, et)
        })

        createExpr(nameResolver, default, exprType).flatMap(ve =>
          converted.flatMap { conv =>
            conv.foldRight(Right(ve): Either[String, Expression[_]]) {
              case ((condition, value), Right(e)) =>
                DataTypeUtils
                  .alignTypes(value, e, calculator)
                  .map(pair => ConditionExpr(condition, pair.a, pair.b).asInstanceOf[Expression[_]])

              case (_, Left(msg)) => Left(msg)
            }
          }
        )

      case parser.FieldName(name) => nameResolver(name)

      case parser.Constant(v) => convertValue(v, exprType)

      case parser.SqlArray(vs) =>
        val consts = CollectionUtils.collectErrors(vs.map(v => convertLiteral(v, exprType)))
        consts.flatMap(createArrayExpr)

      case parser.Tuple(a, b) =>
        for {
          ae <- createExpr(nameResolver, a, exprType)
          be <- createExpr(nameResolver, b, exprType)
        } yield TupleExpr(ae, be)

      case parser.UMinus(a) =>
        createUMinus(nameResolver, a, ExprType.Math)

      case parser.Plus(l, r) =>
        createBinary(nameResolver, l, r, "+", ExprType.Math)

      case parser.Minus(l, r) =>
        createBinary(nameResolver, l, r, "-", ExprType.Math)

      case parser.Multiply(l, r) =>
        createBinary(nameResolver, l, r, "*", ExprType.Math)

      case parser.Divide(l, r) =>
        createBinary(nameResolver, l, r, "/", ExprType.Math)

      case parser.FunctionCall(f, Nil) =>
        FunctionRegistry.nullary(f)

      case parser.Eq(l, r) =>
        createBinaryBool(nameResolver, l, r, "=")

      case parser.Ne(l, r) =>
        createBinaryBool(nameResolver, l, r, "<>")

      case parser.Ge(l, r) =>
        createBinaryBool(nameResolver, l, r, ">=")

      case parser.Gt(l, r) =>
        createBinaryBool(nameResolver, l, r, ">")

      case parser.Le(l, r) =>
        createBinaryBool(nameResolver, l, r, "<=")

      case parser.Lt(l, r) =>
        createBinaryBool(nameResolver, l, r, "<")

      case parser.IsNull(e) =>
        createExpr(nameResolver, e, ExprType.Math).map(ne => IsNullExpr(ne))

      case parser.IsNotNull(e) =>
        createExpr(nameResolver, e, ExprType.Math).map(nne => IsNotNullExpr(nne))

      case parser.In(e, vs) =>
        createExpr(nameResolver, e, ExprType.Cmp).flatMap {
          case ce: Expression[t] =>
            CollectionUtils
              .collectErrors(vs.map(v => convertValue(v, ExprType.Cmp, ce.dataType)))
              .map(cvs => InExpr(ce, cvs.toSet))
        }

      case parser.NotIn(e, vs) =>
        createExpr(nameResolver, e, ExprType.Cmp).flatMap {
          case ce: Expression[t] =>
            CollectionUtils
              .collectErrors(vs.map(v => convertValue(v, ExprType.Cmp, ce.dataType)))
              .map(cvs => NotInExpr(ce, cvs.toSet))
        }
      case parser.And(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(nameResolver, c))).map(AndExpr)

      case parser.Or(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(nameResolver, c))).map(OrExpr)

      case parser.BetweenCondition(e, f, t) =>
        createExpr(nameResolver, e, ExprType.Cmp).flatMap {
          case ex: Expression[t] =>
            for {
              from <- convertValue(f, exprType, Some(ex.dataType))
              to <- convertValue(t, exprType, Some(ex.dataType))
              ge <- createBooleanExpr(ex, from, ">=")
              le <- createBooleanExpr(ex, to, "<=")
            } yield AndExpr(Seq(ge, le))
        }

      case parser.FunctionCall(f, e :: Nil) =>
        for {
          ex <- createExpr(nameResolver, e, exprType)
          fexpr <- FunctionRegistry.unary(f, calculator, ex)
        } yield fexpr

      case parser.FunctionCall(f, e1 :: e2 :: Nil) =>
        for {
          a <- createExpr(nameResolver, e1, exprType)
          b <- createExpr(nameResolver, e2, exprType)
          fexpr <- FunctionRegistry.bi(f, calculator, a, b)
        } yield fexpr

      case parser.FunctionCall(f, _) =>
        Left(s"Undefined function $f")

      case parser.CastExpr(e, t) =>
        for {
          ex <- createExpr(nameResolver, e, exprType)
          tpe <- createType(t)
          c <- DataTypeUtils.exprCast(ex, tpe.aux, calculator)
        } yield c
    }

    e.map {
      case die: DimensionIdExpr => die
      case ex if exprType == ExprType.Cmp && ex.dataType == DataType[String] && ex.kind != Const =>
        LowerExpr(ex.asInstanceOf[Expression[String]])
      case ex => ex
    }
  }

  private def createType(name: String): Either[String, DataType] = {
    DataType.bySqlName(name).toRight(s"Unknown type $name")
  }

  private def createUMinus(
      resolver: NameResolver,
      expr: parser.SqlExpr,
      exprType: ExprType
  ): Either[String, Expression[_]] = {
    for {
      e <- createExpr(resolver, expr, exprType)
      u <- FunctionRegistry.unary("-", calculator, e)
    } yield u
  }

  private def createArrayExpr(expressions: Seq[ValueExpr[_]]): Either[String, Expression[_]] = {
    // we assume all expressions have exact same type, but it might require to align type in future
    val first = expressions.head

    val incorrectType = expressions.collect {
      case x if x.dataType != first.dataType => x
    }

    if (incorrectType.isEmpty) {
      Right(
        ArrayExpr[first.dataType.T](expressions.asInstanceOf[Seq[Expression[first.dataType.T]]])(
          first.dataType
        )
      )
    } else {
      val err = incorrectType.map(e => s"$e has type ${e.dataType}").mkString(", ")
      Left(s"All expressions must have same type but: $err")
    }
  }

  private def createBinary(
      nameResolver: NameResolver,
      l: parser.SqlExpr,
      r: parser.SqlExpr,
      fun: String,
      exprType: ExprType
  ): Either[String, Expression[_]] =
    for {
      le <- createExpr(nameResolver, l, exprType)
      re <- createExpr(nameResolver, r, exprType)
      biFunction <- FunctionRegistry.bi(fun, calculator, le, re)
    } yield biFunction

  private def createBooleanExpr(
      l: Expression[_],
      r: Expression[_],
      fun: String
  ): Either[String, Expression[Boolean]] = {
    FunctionRegistry.bi(fun, calculator, l, r).flatMap { e =>
      if (e.dataType == DataType[Boolean]) Right(e.asInstanceOf[Expression[Boolean]])
      else Left(s"$fun result has type ${e.dataType.meta.sqlType} but BOOLEAN required")
    }
  }

  private def createBinaryBool(
      resolver: NameResolver,
      a: parser.SqlExpr,
      b: parser.SqlExpr,
      cmpName: String
  ): Either[String, Condition] = {
    for {
      l <- createExpr(resolver, a, ExprType.Cmp)
      r <- createExpr(resolver, b, ExprType.Cmp)
      op <- createBooleanExpr(l, r, cmpName)
    } yield op
  }

  private def createCondition(
      resolver: NameResolver,
      e: parser.SqlExpr
  ): Either[String, Condition] = {
    createExpr(resolver, e, ExprType.Cmp).flatMap(e =>
      if (e.dataType == DataType[Boolean]) Right(e.asInstanceOf[Condition])
      else Left(s"$e has type ${e.dataType}, but BOOLEAN is required")
    )
  }

  private def convertValue[T](
      v: parser.Value,
      exprType: ExprType,
      tpe: DataType.Aux[T]
  ): Either[String, ValueExpr[T]] = {
    v match {
      case parser.Placeholder(id) => Right(PlaceholderExpr(id, tpe))
      case l: parser.Literal =>
        val v = convertLiteral(l, exprType)
        v.flatMap(vv => DataTypeUtils.valueCast(vv, tpe, calculator))

      case parser.TupleValue(a, b) =>
        for {
          ae <- convertValue(a, exprType, None)
          be <- convertValue(b, exprType, None)
          tuple = TupleValueExpr(ae, be)
          r <- DataTypeUtils.valueCast(tuple, tpe, calculator)
        } yield r
    }
  }

  private def convertValue(
      v: parser.Value,
      exprType: ExprType,
      tpe: Option[DataType] = None
  ): Either[String, ValueExpr[_]] = {
    v match {
      case parser.Placeholder(id) =>
        tpe match {
          case Some(t) => Right(PlaceholderExpr(id, t.aux))
          case None    => Right(UntypedPlaceholderExpr(id))
        }

      case parser.TupleValue(a, b) =>
        for {
          ae <- convertValue(a, exprType, None)
          be <- convertValue(b, exprType, None)
        } yield TupleValueExpr(ae, be)

      case l: parser.Literal => convertLiteral(l, exprType)
    }
  }

  private def convertLiteral(v: parser.Literal, exprType: ExprType): Either[String, ValueExpr[_]] = {
    v match {
      case tv @ parser.TypedValue(s) if tv.dataType == DataType[String] =>
        val const = if (exprType == ExprType.Cmp) s.asInstanceOf[String].toLowerCase else s.asInstanceOf[String]
        Right(ConstantExpr(const))

      case tv @ parser.TypedValue(t) =>
        Right(ConstantExpr(t)(tv.dataType))

      case parser.NullValue =>
        Right(NullExpr(DataType[Null]))

      case parser.PeriodValue(p) if exprType == ExprType.Cmp =>
        if (p.getPeriod.getYears == 0 && p.getPeriod.getMonths == 0) {
          Right(ConstantExpr(p.getDuration.plusDays(p.getPeriod.getDays).toMillis))
        } else {
          Left(s"Period $p cannot be used as duration, because it has months or years")
        }

      case parser.PeriodValue(p) => Right(ConstantExpr(p))
    }
  }

  private def getFilter(
      table: Option[Table],
      fields: Seq[QueryField],
      condition: Option[parser.SqlExpr]
  ): Either[String, Option[Condition]] = {
    val resolver = table.map(t => fieldByRef(t, fields)(_)).getOrElse(constOrRef(fields)(_))
    condition match {
      case Some(c) => createCondition(resolver, c).map(Some(_))
      case None    => Right(None)
    }
  }

  private def getPostFilter(
      table: Option[Table],
      fields: Seq[QueryField],
      condition: Option[parser.SqlExpr]
  ): Either[String, Option[Condition]] = {
    val resolver = table.map(t => fieldByRef(t, fields)(_)).getOrElse(constOrRef(fields)(_))
    condition match {
      case Some(c) => createCondition(resolver, c).map(Some(_))
      case None    => Right(None)
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

  private def getGroupBy(select: parser.Select, table: Option[Table]): Either[String, Seq[Expression[_]]] = {
    val filled = substituteGroupings(select)
    val resolver = table.map(fieldByName).getOrElse(constOnly)

    val groupBy = filled.map { sqlExpr =>
      createExpr(resolver, sqlExpr, ExprType.Math)
    }

    CollectionUtils.collectErrors(groupBy)
  }

  private val constOnly: NameResolver = name => Left(s"Unknown field $name")

  private def fieldByName(table: Table)(name: String): Either[String, Expression[_]] = {
    val lowerName = name.toLowerCase
    if (lowerName == TIME_FIELD) {
      Right(TimeExpr)
    } else {
      getMetricExpr(table, lowerName) orElse getDimExpr(table, lowerName) orElse getLinkExpr(
        table,
        name
      ) toRight s"Unknown field $name"
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
          case Right(LinkExpr(_, _)) => Left(s"External link field $name cannot be upserted")
          case Right(x)              => Right(x)
          case err                   => err
        }
      }
    )
    exprs.map(_.zipWithIndex.toMap)
  }

  private def getValues(
      values: Seq[(parser.Value, DataType)],
      parameters: Map[Int, Parameter]
  )(implicit srw: StringReaderWriter): Either[String, Array[ConstantExpr[_]]] = {
    val vs = values.map {
      case (v, t) =>
        convertValue(v, ExprType.Math, Some(t)).flatMap {
          case c @ ConstantExpr(_) => Right(c)
          case PlaceholderExpr(id, dt) =>
            parameters
              .get(id)
              .map {
                case p @ TypedParameter(v) =>
                  DataTypeUtils.constCast(v, p.dataType, dt.aux, calculator).map(x => ConstantExpr(x)(dt))
                case UntypedParameter(v) => Right(ConstantExpr(dt.storable.readString(v))(dt))
              }
              .getOrElse(Left(s"No parameter value for #$id"))
          case UntypedPlaceholderExpr(id) => Left(s"Cannot deduce type for placeholder $id")
          case x                          => Left(s"Unexpected expression $x")
        }
    }
    CollectionUtils.collectErrors(vs).map(_.toArray)
  }

  private def getTimeValue(fieldMap: Map[Expression[_], Int], values: Array[ConstantExpr[_]]): Either[String, Long] = {
    val idx = fieldMap.get(TimeExpr).toRight("time field is not defined")
    idx.map(values).flatMap(c => DataTypeUtils.constCast(c, DataType[Time], calculator)).map(_.millis)
  }

  private def getDimensionValues(
      table: Table,
      fieldMap: Map[Expression[_], Int],
      values: Array[ConstantExpr[_]]
  ): Either[String, Map[Dimension, _]] = {
    val dimValues = table.dimensionSeq.map { dim =>
      val idx = fieldMap.get(DimensionExpr(dim.aux)).toRight(s"${dim.name} is not defined")
      idx.map(values).flatMap(c => DataTypeUtils.constCast(c, dim.dataType, calculator)).map(dim -> _)
    }

    CollectionUtils.collectErrors(dimValues).map(_.toMap)
  }

  private def getMetricValues(
      fieldMap: Map[Expression[_], Int],
      values: Array[ConstantExpr[_]]
  ): Either[String, Seq[MetricValue]] = {
    val vs = fieldMap.collect {
      case (MetricExpr(m), idx) =>
        val x: Either[String, Any] = DataTypeUtils.constCast(values(idx), m.dataType, calculator)
        x.map(v => MetricValue(m, v))
    }

    CollectionUtils.collectErrors(vs.toSeq)
  }
}

object SqlQueryProcessor {
  type NameResolver = String => Either[String, Expression[_]]
  val TIME_FIELD: String = Table.TIME_FIELD_NAME

  object ExprType extends Enumeration {
    type ExprType = Value
    val Cmp, Math = Value
  }
}
