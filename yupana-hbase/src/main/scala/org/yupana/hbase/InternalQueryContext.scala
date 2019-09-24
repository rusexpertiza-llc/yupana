package org.yupana.hbase

import org.yupana.api.query.DimensionExpr
import org.yupana.core.model.InternalQuery
import org.yupana.api.schema.{Dimension, Metric}

import scala.collection.mutable

case class InternalQueryContext(query: InternalQuery,
                                fieldIndexMap: mutable.HashMap[Byte, Metric],
                                dimIndexMap: mutable.Map[Dimension, Int],
                                requiredDims: Set[Dimension]
                        )

object InternalQueryContext {
  def apply(query: InternalQuery): InternalQueryContext = {
    val fieldIndexMap = mutable.HashMap(query.table.metrics.map(f => f.tag -> f): _*)

    val dimIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)

    val requiredTags = query.exprs.collect {
      case DimensionExpr(tag) => tag
    }

    new InternalQueryContext(query, fieldIndexMap, dimIndexMap, requiredTags)
  }
}
