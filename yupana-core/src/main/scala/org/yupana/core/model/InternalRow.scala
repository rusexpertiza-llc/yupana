package org.yupana.core.model

import org.yupana.api.query.Expression
import org.yupana.core.QueryContext

class InternalRow(val data: Array[Option[Any]]) extends Serializable {

  def set(queryContext: QueryContext, expr: Expression, v: Option[Any]): InternalRow = {
    data(queryContext.exprsIndex(expr)) = v
    this
  }

  def set(exprIndex: scala.collection.Map[Expression, Int], expr: Expression, v: Option[Any]): InternalRow = {
    data(exprIndex(expr)) = v
    this
  }

  def set(index: Int, v: Option[Any]): InternalRow = {
    data(index) = v
    this
  }

  def get[T](queryContext: QueryContext, expr: Expression): Option[T] = {
    data(queryContext.exprsIndex(expr)).asInstanceOf[Option[T]]
  }

  def get[T](exprIndex: scala.collection.Map[Expression, Int], expr: Expression): Option[T] = {
    data(exprIndex(expr)).asInstanceOf[Option[T]]
  }

  def get[T](index: Int): Option[T] = {
    data(index).asInstanceOf[Option[T]]
  }

  def copy: InternalRow = {
    val dataCopy = Array.ofDim[Option[Any]](data.length)
    Array.copy(data, 0, dataCopy, 0, data.length)
    new InternalRow(dataCopy)
  }
}

class InternalRowBuilder(exprIndex: scala.collection.Map[Expression, Int]) extends Serializable {
  private val data = Array.fill(exprIndex.size)(Option.empty[Any])

  def this(queryContext: QueryContext) = this(queryContext.exprsIndex)

  def set(expr: Expression, v: Option[Any]): InternalRowBuilder = {
    data(exprIndex(expr)) = v
    this
  }

  def buildAndReset(): InternalRow = {
    val dataCopy = Array.ofDim[Option[Any]](data.length)
    Array.copy(data, 0, dataCopy, 0, data.length)
    val result = new InternalRow(dataCopy)

    data.indices.foreach(i => data(i) = None)

    result
  }
}
