package org.yupana.core.model

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.yupana.core.QueryContext

class KeyData(@transient val queryContext: QueryContext, @transient val row: InternalRow) extends Serializable {

  private var data: Array[Option[Any]] = _

  override def hashCode(): Int = {
    import scala.util.hashing.MurmurHash3._

    if (queryContext != null) {
      val h = queryContext.groupByExprs.foldLeft(0xe73b8c13) { (h, e) =>
        mix(h, row.get(queryContext, e).##)
      }
      finalizeHash(h, queryContext.groupByExprs.length)
    } else {
      arrayHash(data)
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: KeyData =>
        if (this eq that) {
          true
        } else if (this.queryContext != null && that.queryContext != null) {
          queryContext.groupByExprs.foldLeft(true) { (a, e) =>
            val i = queryContext.exprsIndex(e)
            a && this.row.get(i) == that.row.get(i)
          }
        } else if (this.queryContext != null) {
          queryContext.groupByExprs.indices.forall(idx =>
            this.row.get(queryContext, queryContext.groupByExprs(idx)) == that.data(idx)
          )
        } else if (that.queryContext != null) {
          that.queryContext.groupByExprs.indices.forall(idx =>
            that.row.get(queryContext, queryContext.groupByExprs(idx)) == this.data(idx)
          )
        } else {
          this.data sameElements that.data
        }

      case _ => false
    }
  }

  private def calcData: Array[Option[Any]] = {
    val keyData = Array.ofDim[Option[Any]](queryContext.groupByExprs.length)

    keyData.indices foreach { i =>
      keyData(i) = row.get(queryContext, queryContext.groupByExprs(i))
    }

    keyData
  }

  // Overriding Java serialization
  private def writeObject(oos: ObjectOutputStream): Unit = {
    if (data == null) data = calcData
    oos.writeObject(data)
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    data = ois.readObject().asInstanceOf[Array[Option[Any]]]
  }
}
