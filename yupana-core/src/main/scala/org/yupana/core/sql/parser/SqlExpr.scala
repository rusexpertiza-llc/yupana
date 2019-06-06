package org.yupana.core.sql.parser

sealed trait SqlExpr {
  def proposedName: Option[String]
}

case class Constant(value: Value) extends SqlExpr {
  override def proposedName: Option[String] = Some(value.asString)
}

case class FieldName(name: String) extends SqlExpr {
  val lowerName: String = name.toLowerCase
  override val proposedName: Option[String] = Some(name)
}

case class FunctionCall(function: String, exprs: List[SqlExpr]) extends SqlExpr {
  override def proposedName: Option[String] = {
    val exprsNames = exprs.flatMap(_.proposedName)
    if (exprsNames.size == exprs.size) {
      val args = exprsNames.mkString(",")
      Some(s"$function($args)")
    } else None
  }
}

case class Case(conditionalValues: Seq[(Condition, SqlExpr)], defaultValue: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = None
}

case class Plus(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = for {
    aName <- a.proposedName
    bName <- b.proposedName
  } yield s"${aName}_plus_$bName"
}

case class Minus(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = for {
    aName <- a.proposedName
    bName <- b.proposedName
  } yield s"${aName}_minus_$bName"
}

case class Multiply(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = for {
    aName <- a.proposedName
    bName <- b.proposedName
  } yield s"${aName}_mul_$bName"
}

case class Divide(a: SqlExpr, b: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = for {
    aName <- a.proposedName
    bName <- b.proposedName
  } yield s"${aName}_div_$bName"
}

case class UMinus(x: SqlExpr) extends SqlExpr {
  override def proposedName: Option[String] = x.proposedName.map(n => s"minus_$n")
}
