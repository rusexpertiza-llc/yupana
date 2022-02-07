package org.yupana.rocks

import org.yupana.api.schema.{ DictionaryDimension, Dimension, HashDimension, RawDimension, Table }
import org.yupana.api.utils.PrefetchedSortedSetIterator

import java.nio.ByteBuffer
import scala.collection.AbstractIterator

class PrefixIterator(table: Table, rangeScanDims: Seq[(Dimension, PrefetchedSortedSetIterator[_])])
    extends AbstractIterator[Array[Byte]] {

  private val prefixSize = {
    1 + // table id
      rangeScanDims.map(_._1.rStorable.size).sum // dimensions
  }

  private val dimensionsAndValues = rangeScanDims.toArray.zipWithIndex.map {
    case ((dim, it), idx) => (dim, it.fetched, idx)
  }
  private val indexes = Array.ofDim[Int](dimensionsAndValues.length)

  private var nextFlag = dimensionsAndValues.exists { case (_, values, idx) => indexes(idx) < values.length }

  override def hasNext: Boolean = {
    nextFlag
  }

  override def next(): Array[Byte] = {

    def loop(i: Int): Unit = {
      val (_, values, _) = dimensionsAndValues(i)
      if (indexes(i) < values.length - 1) {
        indexes(i) += 1
      } else {
        indexes(i) = 0
        if (i > 0) {
          loop(i - 1)
        } else {
          nextFlag = false
        }
      }
    }

    if (!hasNext) throw new IllegalStateException("Next on empty iterator")
    val prefix = prefixBytes
    loop(indexes.length - 1)
    prefix
  }

  private def prefixBytes: Array[Byte] = {

    val buffer = ByteBuffer
      .allocate(prefixSize)
      .put(table.id)

    dimensionsAndValues.foreach {
      case (dim, values, idx) =>
        val anyValue = values(indexes(idx))

        val bytes = dim match {
          case _: DictionaryDimension =>
            Array.ofDim[Byte](java.lang.Long.BYTES)

          case rd: RawDimension[_] =>
            val v = anyValue.asInstanceOf[rd.T]
            rd.rStorable.write(v)

          case hd: HashDimension[_, _] =>
            val v = anyValue.asInstanceOf[hd.T]
            val hash = hd.hashFunction(v).asInstanceOf[hd.R]
            hd.rStorable.write(hash)
        }

        buffer.put(bytes)
    }
    buffer.array()
  }
}
