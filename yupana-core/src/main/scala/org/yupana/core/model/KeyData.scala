package org.yupana.core.model

import org.yupana.api.query.Expression
import org.yupana.core.QueryContext

class KeyData(queryContext: QueryContext, val valueData: Array[Option[Any]]) extends Serializable {

  override def hashCode(): Int = {
    import scala.util.hashing.MurmurHash3._

    val h = queryContext.groupByArray.foldLeft(0xe73b8c13){ (h, e) =>
      mix(h, valueData(queryContext.exprsIndex(e)).##)
    }
    finalizeHash(h, queryContext.groupByArray.length)
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: KeyData =>
        queryContext.groupByArray.foldLeft(true){ (a, e) =>
          val i = queryContext.exprsIndex(e)
          a && this.valueData(i) == that.valueData(i)
        }
      case _ => false
    }
  }

  def get[T](expr: Expression): Option[T] = {
    valueData(queryContext.exprsIndex(expr)).asInstanceOf[Option[T]]
  }
}
