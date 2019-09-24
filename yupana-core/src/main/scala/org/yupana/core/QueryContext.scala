package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query._

import scala.collection.mutable

case class QueryContext(query: Query,
                        postCondition: Option[Condition],
                        postConditionExprs: Array[Expression],
                        exprsIndex: mutable.HashMap[Expression, Int],
                        aggregateExprs: Array[AggregateExpr],
                        topRowExprs: Array[Expression],
                        exprsOnAggregatesAndWindows: Array[Expression],
                        bottomExprs: Array[Expression],
                        linkExprs: Seq[LinkExpr],
                        groupByExprs: Array[Expression]
)

object QueryContext extends StrictLogging {

  def apply(query: Query, postCondition: Condition): QueryContext = {
    val conditionExprs = postCondition.exprs

    val requiredTags = query.groupBy.flatMap(_.requiredDimensions).toSet ++
      query.fields.flatMap(_.expr.requiredDimensions).toSet ++
      conditionExprs.flatMap(_.requiredDimensions) ++
      query.postFilter.toSeq.flatMap(_.exprs.flatMap(_.requiredDimensions))

    val requiredDimExprs = requiredTags.map(DimensionExpr(_))

    val groupByExternalLinks = query.groupBy.flatMap(_.requiredLinks)
    val fieldsExternalLinks = query.fields.flatMap(_.expr.requiredLinks)
    val dataFiltersExternalLinks = conditionExprs.flatMap(_.requiredLinks)
    val havingExternalLinks = query.postFilter.toSeq.flatMap(_.exprs.flatMap(_.requiredLinks))

    val linkExprs = (groupByExternalLinks ++ fieldsExternalLinks ++ dataFiltersExternalLinks ++ havingExternalLinks).distinct

    val topExprs: Set[Expression] = query.fields.map(_.expr).toSet ++
      (query.groupBy.toSet ++
        conditionExprs ++
        requiredDimExprs ++
        query.postFilter.map(_.exprs).getOrElse(Set.empty) +
        TimeExpr
      ).filterNot(_.isInstanceOf[ConstantExpr])

    val topRowExprs: Set[Expression] = topExprs.filter { expr =>
      !expr.isInstanceOf[ConstantExpr] && (
        (!expr.containsAggregates && !expr.containsWindows) ||
          expr.isInstanceOf[AggregateExpr] ||
          expr.isInstanceOf[WindowFunctionExpr]
        )
    }

    val exprsOnAggregatesAndWindows = topExprs diff topRowExprs

    val allExprs: Set[Expression] = topExprs.flatMap(_.flatten)

    val bottomExprs: Set[Expression] = collectBottomExprs(allExprs)

    val aggregateExprs = allExprs.collect { case ae: AggregateExpr => ae }

    val exprsIndex =  mutable.HashMap(allExprs.zipWithIndex.toSeq: _*)

    val optionPost = if (postCondition == EmptyCondition) None else Some(postCondition)

    new QueryContext(
      query,
      optionPost,
      postCondition.exprs.toArray,
      exprsIndex,
      aggregateExprs.toArray,
      (topRowExprs -- bottomExprs).toArray,
      exprsOnAggregatesAndWindows.toArray,
      bottomExprs.toArray,
      linkExprs,
      query.groupBy.toArray
    )
  }

  def collectBottomExprs(exprs: Set[Expression]): Set[Expression] = exprs.collect {
    case a@AggregateExpr(_, e) => Set(a, e)
    case ConditionExpr(condition, _, _) => condition.exprs.flatMap(_.flatten)
    case c: ConstantExpr => Set(c)
    case t: DimensionExpr => Set(t)
    case c: LinkExpr => Set(c)
    case v: MetricExpr[_] => Set(v)
    case TimeExpr => Set(TimeExpr)
    case _ => Set.empty
  }.flatten
}
