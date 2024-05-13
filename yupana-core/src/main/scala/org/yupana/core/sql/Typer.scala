package org.yupana.core.sql

import org.yupana.api.schema.Table
import org.yupana.api.types.DataType
import org.yupana.core.sql.parser._

sealed trait TyperType
case class Arrow(name: String, a: TyperType, b: TyperType) extends TyperType
case class KnownType(name: String, dt: DataType) extends TyperType
case class NumericType(name: String) extends TyperType
case class UnknownType(name: String) extends TyperType
case class ArrayType(name: String, elem: TyperType) extends TyperType

sealed trait ProtoExpr
case class TypedExpr(sqlExpr: SqlExpr, dataType: TyperType) extends ProtoExpr

class Typer(table: Table) {

  private var n = 0
//  private var env = Map.empty[String, TyperType]

  private def newType: String = {
    n += 1
    s"A$n"
  }

  private def fieldType(name: String): Option[DataType] = {
    table.dimensionSeq.find(_.name == name).map(_.dataType) orElse table.metrics.find(_.name == name).map(_.dataType)
  }

  def deduce(expr: SqlExpr): Either[String, ProtoExpr] = {
    expr match {
      case FieldName(name) => fieldType(name).map(t => TypedExpr(expr, KnownType(newType, t))).toRight("Fucked up")
      case Constant(tv @ TypedValue(_)) if tv.dataType.numeric.isDefined => Right(TypedExpr(expr, NumericType(newType)))
      case Constant(tv @ TypedValue(_)) => Right(TypedExpr(expr, KnownType(newType, tv.dataType)))
      case Constant(Placeholder(_))     => Right(TypedExpr(expr, UnknownType(newType)))
      case Plus(a, b) =>                => num2(a,b)

      case _ => ???
    }
  }

  private def num2(a: SqlExpr, b:SqlExpr)
}
