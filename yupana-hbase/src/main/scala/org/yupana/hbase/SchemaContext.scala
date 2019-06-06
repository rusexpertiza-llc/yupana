package org.yupana.hbase

import org.yupana.api.query.DimensionExpr
import org.yupana.core.model.InternalQuery
import org.yupana.api.schema.{Dimension, Metric}

import scala.collection.mutable

case class SchemaContext(query: InternalQuery,
                         fieldIndexMap: mutable.HashMap[Byte, Metric],
                         tagIndexMap: mutable.Map[Dimension, Int],
                         requiredTags: Set[Dimension]
                        )

object SchemaContext {
  def apply(query: InternalQuery): SchemaContext = {
    val fieldIndexMap = mutable.HashMap(query.table.metrics.map(f => f.tag -> f): _*)

    val tagIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)

    val requiredTags = query.exprs.collect {
      case DimensionExpr(tag) => tag
    }

    new SchemaContext(query, fieldIndexMap, tagIndexMap, requiredTags)
  }
}
