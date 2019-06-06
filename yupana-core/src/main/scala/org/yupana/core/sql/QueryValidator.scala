package org.yupana.core.sql

import org.yupana.api.query._

trait QueryValidator {
  def validateQuery(query: Query): Either[String, Query] = {
    validateFields(query)
  }

  private def validateFields(query: Query): Either[String, Query] = {

    val errors = if (query.groupBy.nonEmpty) {
      if (query.fields.exists(_.expr.kind == Window)) {
        query.fields.flatMap {
          case QueryField(name, e) =>
            e.kind match {
              case Aggregate => Some(s"Aggregation defined for field $name together with window functions")
              case Invalid => Some(s"Invalid expression '$e' for field $name")
              case _ => None
            }
        }
      } else {
        query.fields.flatMap {
          case QueryField(name, e) =>
            e.kind match {
              case Const | Aggregate | Window => None
              case Simple if query.groupBy.contains(e) => None
              case Simple => Some(s"Aggregate function is not defined for field $name")
              case Invalid => Some(s"Invalid expression '$e' for field $name")
            }
        }
      }
    } else {
      query.fields.flatMap {
        case QueryField(name, e) =>
          e.kind match {
            case Aggregate => Some(s"Aggregation is defined for field $name without group by")
            case Invalid => Some(s"Invalid expression '$e' for field $name")
            case _ => None
          }
      }
    }

    if (errors.nonEmpty) {
      Left(errors mkString ", ")
    } else  {
      Right(query)
    }
  }
}
