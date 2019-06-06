package org.yupana.core.sql

import org.yupana.core.utils.CollectionUtils
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{Dimension, ExternalLink, Metric, Schema, Table}
import org.yupana.api.types._
import org.yupana.core.sql.SqlQueryProcessor.ExprType.ExprType
import org.yupana.core.sql.parser.{SqlFieldList, SqlFieldsAll}

class SqlQueryProcessor(schema: Schema) {

  import SqlQueryProcessor._

  def createQuery(select: parser.Select, parameters: Map[Int, parser.Value] = Map.empty): Either[String, Query]  = {
    val state = new BuilderState(parameters)
    val query = for {
      schema <- getSchema(select.schemaName).right
      fields <- getFields(schema, select, state).right
      filter <- getFilter(schema, fields, select.condition, state).right
      groupBy <- getGroupBy(select, schema, state).right
      pf <- getPostFilter(schema, fields, select.having, state).right
    } yield {
      Query(schema, fields, filter, groupBy, select.limit, pf)
    }

    query.right.flatMap(validateQuery)
  }

  private def getSchema(schemaName: String): Either[String, Table] = {
    schema.getTable(schemaName).toRight(s"Unknown table '$schemaName'")
  }
}

object SqlQueryProcessor extends QueryValidator {

  type NameResolver = String => Option[Expression]
  val TIME_FIELD: String = Table.TIME_FIELD_NAME

  val function0Registry: Map[String, BuilderState => Expression] = Map(
    "now" -> ((s: BuilderState) => ConstantExpr(Time(s.queryStartTime)))
  )

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

  private def getFields(table: Table, select: parser.Select, state: BuilderState): Either[String, Seq[QueryField]] = {
    select.fields match {
      case SqlFieldList(fs) =>
        val fields = fs.map(f => getField(table, f, state))
        CollectionUtils.collectErrors(fields)

      case SqlFieldsAll =>
        Left("All fields matching is not supported")
    }
  }

  private def getField(table: Table, field: parser.SqlField, state: BuilderState): Either[String, QueryField] = {
    val fieldName = state.fieldName(field)

    createExpr(table, state, fieldByName(table), field.expr, ExprType.Math).right.map(_.as(fieldName))
  }

  private def fieldByRef(table: Table, fields: Seq[QueryField])(name: String): Option[Expression] = {
    fields.find(_.name == name).map(f => f.expr).orElse(fieldByName(table)(name))
  }

  private def createExpr(table: Table,
                         state: BuilderState,
                         nameResolver: NameResolver,
                         expr: parser.SqlExpr,
                         exprType: ExprType): Either[String, Expression] = {
    expr match {
      case parser.Case(cs, default) =>
        val converted = CollectionUtils.collectErrors(cs.map { case (condition, value) =>
          for {
            et <- createExpr(table, state, nameResolver, value, exprType).right
            c <- createCondition(table, state, nameResolver, condition.simplify).right
          } yield (c, et)
        })

        createExpr(table, state, nameResolver, default, exprType).right.flatMap(ve =>
          converted.right.flatMap { conv =>
            conv.foldRight(Right(ve): Either[String, Expression]) {
              case ((condition, value), Right(e)) =>
                ExprPair.alignTypes(value, e).right.map(pair =>
                  ConditionExpr(condition, pair.a, pair.b).asInstanceOf[Expression]
                )

              case (_, Left(msg)) => Left(msg)
            }
          }
        )

      case parser.FieldName(name) =>
        nameResolver(name) toRight s"Unknown field $name"

      case parser.Constant(v) => convertValue(state, v, exprType)

      case parser.UMinus(a) =>
        createUMinus(table, state, nameResolver, a, ExprType.Math)

      case parser.Plus(l, r) =>
        createBinary(table, state, nameResolver, l, r, BinaryOperation.PLUS, ExprType.Math)

      case parser.Minus(l, r) =>
        createBinary(table, state, nameResolver, l, r, BinaryOperation.MINUS, ExprType.Math)

      case parser.Multiply(l, r) =>
        createBinary(table, state, nameResolver, l, r, BinaryOperation.MULTIPLY, ExprType.Math)

      case parser.Divide(l, r) =>
        createBinary(table, state, nameResolver, l, r, BinaryOperation.DIVIDE, ExprType.Math)

      case parser.FunctionCall(f, Nil) =>
        function0Registry.get(f).map(_(state)).toRight(s"Unknown nullary function $f")

      case parser.FunctionCall(f, e :: Nil) =>
        for {
          ex <- createExpr(table, state, nameResolver, e, exprType).right
          fexpr <- createFunctionExpr(f, ex).right
        } yield fexpr

      case parser.FunctionCall(f, e1 :: e2 :: Nil) =>
        for {
          a <- createExpr(table, state, nameResolver, e1, exprType).right
          b <- createExpr(table, state, nameResolver, e2, exprType).right
          fexpr <- createFunction2Expr(f, a, b).right
        } yield fexpr

      case parser.FunctionCall(f, es) =>
        for {
          vs <- CollectionUtils.collectErrors(es.map(e => createExpr(table, state, nameResolver, e, exprType))).right
          fexpr <- createArrayUnaryFunctionExpr(f, vs).right
        } yield fexpr
    }
  }

  private def createUMinus(table: Table, state: SqlQueryProcessor.BuilderState, resolver: NameResolver, expr: parser.SqlExpr, exprType: ExprType): Either[String, Expression] = {
    expr match {
      // TODO: this may be removed when we will calculate constant values before query execution
      case parser.Constant(parser.NumericValue(n)) => Right(ConstantExpr(-n))
      case x =>
        for {
          e <- createExpr(table, state, resolver, expr, exprType).right
          u <- createUnaryFunctionExpr("-", e).right
        } yield u
    }
  }

  private def createFunctionExpr(fun: String, expr: Expression): Either[String, Expression] = {
    for {
      _ <- createWindowFunctionExpr(fun, expr).left
      _ <- createAggregateExpr(fun, expr).left
      m <- createUnaryFunctionExpr(fun, expr).left
      _ <- createArrayUnaryFunctionExpr(fun, Seq(expr)).left
    } yield m
  }

  private def createFunction2Expr(fun: String, e1: Expression, e2: Expression): Either[String, Expression] = {
    for {
      m <- createBiFunction(fun, e1, e2).left
      _ <- createArrayUnaryFunctionExpr(fun, Seq(e1, e2)).left
    } yield m
  }

  private def createArrayUnaryFunctionExpr(functionName: String, expressions: Seq[Expression]): Either[String, Expression] = {
    createArrayExpr(expressions).right.flatMap(e => createUnaryFunctionExpr(functionName, e))
  }

  private def createArrayExpr(expressions: Seq[Expression]): Either[String, Expression] = {
    // we assume all expressions have exact same type, but it might require to align type in future
    val first = expressions.head

    val incorrectType = expressions.collect {
      case x if x.dataType != first.dataType => x
    }

    if (incorrectType.isEmpty) {
      Right(ArrayExpr[first.Out](expressions.toArray.asInstanceOf[Array[Expression.Aux[first.Out]]])(first.dataType))
    } else {
      val err = incorrectType.map(e => s"$e has type ${e.dataType}").mkString(", ")
      Left(s"All expressions must have same type but: $err}")
    }
  }

  private def createAggregateExpr(fun: String, expr: Expression) = {
    val agg = expr.dataType.operations.aggregation(fun).toRight(s"Unknown aggregate function $fun")
    agg.right.map(a => AggregateExpr(a, expr.aux))
  }

  private def createWindowFunctionExpr(fun: String, expr: Expression) = {
    val func = TypeWindowOperations.getFunction(fun, expr.dataType).toRight(s"Unknown window operation $fun")
    func.right.map(f => WindowFunctionExpr(f, expr.aux))
  }

  private def createUnaryFunctionExpr(fun: String, expr: Expression) = {
    val uf = expr.dataType.operations.unaryOperation(fun).toRight(s"Function $fun is not defined on type ${expr.dataType}").right
    uf.map(f => UnaryOperationExpr(f.asInstanceOf[UnaryOperation.Aux[expr.Out, f.Out]], expr.aux).asInstanceOf[Expression])
  }

  private def createBinary(table: Table,
                           state: BuilderState,
                           nameResolver: NameResolver,
                           l: parser.SqlExpr,
                           r: parser.SqlExpr,
                           fun: String,
                           exprType: ExprType): Either[String, Expression] = for {
    le <- createExpr(table, state, nameResolver, l, exprType).right
    re <- createExpr(table, state, nameResolver, r, exprType).right
    biFunction <- createBiFunction(fun, le, re).right
  } yield biFunction

  def createBiFunction(fun: String, l: Expression, r: Expression): Either[String, Expression] = {
    val expr = l.dataType.operations.biOperation(fun, r.dataType)
      .map(op => BinaryOperationExpr[l.Out, r.Out, op.Out](op, l, r))

    expr match {
      case Some(e) => Right(e)
      case None => for {
        pair <- ExprPair.alignTypes(l, r).right
        biOperation <- pair.dataType.operations.biOperation(fun, pair.dataType)
          .toRight(s"Unsupported operation $fun on ${l.dataType} and ${r.dataType}").right
      } yield {
        BinaryOperationExpr[pair.T, pair.T, biOperation.Out](biOperation, pair.a, pair.b).asInstanceOf[Expression]
      }
    }
  }

  def createBooleanExpr(l: Expression, r: Expression, fun: String): Either[String, Expression.Aux[Boolean]] = {
    createBiFunction(fun, l, r).right.flatMap { e =>
      if (e.dataType == DataType[Boolean]) Right(e.asInstanceOf[Expression.Aux[Boolean]])
      else Left(s"$fun result type is ${e.dataType.meta.sqlType} but BOOLEAN required")
    }
  }

  private def createCondition(table: Table,
                              state: BuilderState,
                              nameResolver: NameResolver,
                              c: parser.Condition): Either[String, Condition] = {

    def construct(cmpName: String, a: parser.SqlExpr, b: parser.SqlExpr): Either[String, Condition] = {
      for {
        l <- createExpr(table, state, nameResolver, a, ExprType.Cmp).right
        r <- createExpr(table, state, nameResolver, b, ExprType.Cmp).right
        op <- createBooleanExpr(l, r, cmpName).right
      } yield SimpleCondition(op)
    }

    c match {
      case parser.Eq(e, v) => construct(BinaryOperation.EQ, e, v)
      case parser.Ne(e, v) => construct(BinaryOperation.NE, e, v)
      case parser.Lt(e, v) => construct(BinaryOperation.LT, e, v)
      case parser.Gt(e, v) => construct(BinaryOperation.GT, e, v)
      case parser.Le(e, v) => construct(BinaryOperation.LE, e, v)
      case parser.Ge(e, v) => construct(BinaryOperation.GE, e, v)

      case parser.IsNull(e) => for {
        ne <- createExpr(table, state, nameResolver, e, ExprType.Math).right
      } yield SimpleCondition(UnaryOperationExpr(UnaryOperation.isNull, ne.aux))

      case parser.IsNotNull(e) => for {
        nne <- createExpr(table, state, nameResolver, e, ExprType.Math).right
      } yield SimpleCondition(UnaryOperationExpr(UnaryOperation.isNotNull, nne.aux))

      case parser.In(e, vs) =>
        for {
          ce <- createExpr(table, state, nameResolver, e, ExprType.Cmp).right
          cvs <- CollectionUtils.collectErrors(vs.map(v => convertValue(state, v, ce.dataType))).right
        } yield In(ce.aux, cvs.toSet).asInstanceOf[Condition]

      case parser.NotIn(e, vs) =>
        for {
          ce <- createExpr(table, state, nameResolver, e, ExprType.Cmp).right
          cvs <- CollectionUtils.collectErrors(vs.map(v => convertValue(state, v, ce.dataType))).right
        } yield NotIn(ce.aux, cvs.toSet)

      case parser.And(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(table, state, nameResolver, c))).right.map(And)

      case parser.Or(cs) =>
        CollectionUtils.collectErrors(cs.map(c => createCondition(table, state, nameResolver, c))).right.map(Or)

      case parser.ExprCondition(e) =>
        createExpr(table, state, nameResolver, e, ExprType.Cmp).right.flatMap { ex =>
          if (ex.dataType == DataType[Boolean]) {
            Right(SimpleCondition(ex.asInstanceOf[Expression.Aux[Boolean]]))
          } else {
            Left(s"$ex has type ${ex.dataType}, but BOOLEAN is required")
          }
        }
    }
  }

  class TypedValue[T](v: T, dataType: DataType.Aux[T])

  private def convertValue(state: BuilderState, v: parser.Value, dataType: DataType): Either[String, dataType.T] = {
    convertValue(state, v, ExprType.Cmp) match {
      case Right(const) =>
        if (const.dataType == dataType) {
          Right(const.v.asInstanceOf[dataType.T])
        } else {
          TypeConverter(const.dataType, dataType.aux)
            .map(conv => conv.direct(const.v))
            .orElse(TypeConverter(dataType.aux, const.dataType)
              .flatMap(conv => conv.reverse(const.v))
            ).toRight(s"Cannot convert ${const.dataType.meta.sqlTypeName} to ${dataType.meta.sqlTypeName}")
        }

      case Left(e) => Left(e)
    }

  }

  private def convertValue(state: BuilderState, v: parser.Value, exprType: ExprType): Either[String, ConstantExpr] = {
    v match {
      case parser.StringValue(s) =>
        Right(ConstantExpr(s))

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

  private def getFilter(table: Table,
                        fields: Seq[QueryField],
                        condition: Option[parser.Condition],
                        state: BuilderState): Either[String, Condition] = {
    condition match {
      case Some(c) =>
        createCondition(table, state, fieldByRef(table, fields), c.simplify)
      case None => Left("WHERE condition should be non-empty")
    }
  }

  private def getPostFilter(table: Table,
                            fields: Seq[QueryField],
                            condition: Option[parser.Condition],
                            state: BuilderState): Either[String, Option[Condition]] = {
    condition match {
      case Some(c) =>
        createCondition(table, state, fieldByRef(table, fields), c.simplify).right.map(Some(_))
      case None => Right(None)
    }
  }

  private def substituteGroupings(select: parser.Select): Seq[parser.SqlExpr] = {
    select.groupings.map {
      case g@parser.FieldName(n) =>
        select.fields match {
          case SqlFieldList(fields) => fields.find(_.alias.contains(n)).map(_.expr).getOrElse(g)
          case SqlFieldsAll => g
        }
      case x => x
    }
  }

  private def getGroupBy(select: parser.Select, table: Table, state: BuilderState): Either[String, Seq[Expression]] = {
    val filled = substituteGroupings(select)


    val groupBy = filled.map { sqlExpr =>
      createExpr(table, state, fieldByName(table), sqlExpr, ExprType.Math)
    }

    CollectionUtils.collectErrors(groupBy)
  }

  private def findCatalogField(table: Table, field: String): Option[(ExternalLink, String)] = {
    val pos = field.indexOf('_')

    if (pos > 0) {
      val catName = field.substring(0, pos)
      val catField = field.substring(pos + 1)
      for {
        c <- table.externalLinks.find(_.linkName equalsIgnoreCase catName)
        f <- c
          .fieldsNames.find(_ equalsIgnoreCase catField)
          .orElse(if (c.hasDynamicFields) Some(catField) else None)
      } yield (c, f)
    } else {
      None
    }
  }

  private def findDimension(table: Table, name: String): Option[Dimension] = {
    table.dimensionSeq.find(_.name.toLowerCase == name)
  }

  private def fieldByName(table: Table)(name: String): Option[Expression] = {
    val lowerName = name.toLowerCase
    if (lowerName == TIME_FIELD) {
      Some(TimeExpr)
    } else {
      getMetricExpr(table, lowerName) orElse getTagExpr(table, lowerName) orElse getCatalogExpr(table, name)
    }
  }

  private def getMetricExpr(table: Table, fieldName: String): Option[MetricExpr[_]] = {
    table.metrics.find(_.name.toLowerCase == fieldName).map(f => MetricExpr(f.asInstanceOf[Metric.Aux[f.T]]))
  }

  private def getTagExpr(table: Table, fieldName: String): Option[DimensionExpr] = {
    findDimension(table, fieldName).map(new DimensionExpr(_))
  }

  private def getCatalogExpr(table: Table, fieldName: String): Option[LinkExpr] = {
    findCatalogField(table, fieldName).map {case (catalog, field) => new LinkExpr(catalog, field)}
  }
}
