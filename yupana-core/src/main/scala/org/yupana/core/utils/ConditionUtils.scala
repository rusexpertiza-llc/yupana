package org.yupana.core.utils

import org.yupana.api.query._

object ConditionUtils {
  def simplify(condition: Condition): Condition = {
    condition match {
      case And(cs) => Condition.and(cs.flatMap(optimizeAnd))
      case Or(cs) => Condition.or(cs.flatMap(optimizeOr))
      case c => c
    }
  }

  def flatMap(c: Condition)(f: Condition => Condition): Condition = {
    def doFlat(xs: Seq[Condition]): Seq[Condition] =  {
      xs.flatMap(x => flatMap(x)(f) match {
        case EmptyCondition => None
        case nonEmpty => Some(nonEmpty)
      })
    }

    c match {
      case And(cs) => Condition.and(doFlat(cs))
      case Or(cs) => Condition.or(doFlat(cs))
      case x => f(x)
    }
  }


  def isTimeLimit(c: Condition): Boolean = {
    c match {
      case SimpleCondition(BinaryOperationExpr(op, _: TimeExpr.type, ConstantExpr(_))) => Set(">=", ">", "<=", "<").contains(op.name)
      case _ => false
    }
  }

  def isCatalogFilter(c: Condition): Boolean = {
    c match {
      case SimpleCondition(BinaryOperationExpr(op, _: LinkExpr, ConstantExpr(_))) => Set("==", "!=").contains(op.name)
      case In(_: LinkExpr, _) => true
      case NotIn(_: LinkExpr, _) => true
    }
  }

  def merge(a: Condition, b: Condition): Condition = {
    (a, b) match {
      case (EmptyCondition, x) => x
      case (x, EmptyCondition) => x
      case (And(as), And(bs)) => And((as ++ bs).distinct)
      case (_, Or(_)) => throw new IllegalArgumentException("OR is not supported yet")
      case (Or(_), _) => throw new IllegalArgumentException("OR is not supported yet")
      case (And(as), _) => And((as :+ b).distinct)
      case (_, And(bs)) => And((a +: bs).distinct)
      case _ => And(Seq(a, b))
    }
  }


  def split(c: Condition)(p: Condition => Boolean): (Condition, Condition) = {
    def doSplit(c: Condition): (Condition, Condition) = {
      c match {
        case And(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (And(a), And(b))

        case Or(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (Or(a), Or(b))

        case x => if (p(x)) (x, EmptyCondition) else (EmptyCondition, x)
      }
    }

    val (a, b) = doSplit(c)

    (simplify(a), simplify(b))
  }

  def extractValues[T](condition: Condition, pf: PartialFunction[Condition, Seq[T]]): Seq[T] = {
    pf.applyOrElse(condition, (c: Condition) => c match {
      case Or(_) => throw new IllegalArgumentException("OR is not supported yet")
      case And(cs) => cs.flatMap(c => extractValues(c, pf))
      case _ => Seq.empty
    })
  }

  private def optimizeAnd(c: Condition): Seq[Condition] = {
    c match {
      case And(cs) => cs.flatMap(optimizeAnd)
      case x => Seq(simplify(x))
    }
  }

  private def optimizeOr(c: Condition): Seq[Condition] = {
    c match {
      case Or(cs) => cs.flatMap(optimizeOr)
      case x => Seq(simplify(x))
    }
  }
}
