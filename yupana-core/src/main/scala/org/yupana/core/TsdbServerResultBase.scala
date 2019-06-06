package org.yupana.core

trait TsdbServerResultBase {

  protected val nameIndex: Seq[(String, Int)] = queryContext.query.fields.map(f => f.name -> queryContext.exprsIndex(f.expr))
  protected lazy val nameIndexMap: Map[String, Int] = nameIndex.toMap
  protected lazy val fieldIndex: Array[Int] = nameIndex.map(_._2).toArray

  def queryContext: QueryContext

  def dataIndexForFieldName(name: String): Int = nameIndexMap(name)

  def dataIndexForFieldIndex(idx: Int): Int = fieldIndex(idx)
}
