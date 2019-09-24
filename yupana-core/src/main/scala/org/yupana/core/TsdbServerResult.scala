package org.yupana.core

import org.yupana.api.query.Result
import org.yupana.api.types.DataType

class TsdbServerResult(override val queryContext: QueryContext, data: Iterator[Array[Option[Any]]])
  extends Result with TsdbServerResultBase[Iterator] {

  override def rows: Iterator[Array[Option[Any]]] = data

  override val dataTypes: Seq[DataType] = queryContext.query.fields.map(_.expr.dataType)
  override val fieldNames: Seq[String] = nameIndex.map(_._1)
}
