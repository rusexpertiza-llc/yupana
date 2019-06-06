package org.yupana.api.query

sealed trait ExprKind
case object Const extends ExprKind
case object Simple extends ExprKind
case object Aggregate extends ExprKind
case object Window extends ExprKind
case object Invalid extends ExprKind

object ExprKind {
  def combine(a: ExprKind, b: ExprKind): ExprKind = {
    (a, b) match {
      case (x, y) if x == y => x
      case (Invalid, _) => Invalid
      case (_, Invalid) => Invalid
      case (x, Const) => x
      case (Const, x) => x
      case (Window, Simple) => Window
      case (Simple, Window) => Window
      case _ => Invalid
    }
  }
}
