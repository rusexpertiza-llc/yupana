package org.yupana.hbase

case class TSDOutputRow[T](
  key: TSDRowKey[T],
  values: Array[(Long, Array[Byte])]
) {
  override def toString: String = {
    s"""TSDRow(
      | key: ${key.baseTime} / ${key.dimIds.toList}
      | values: ${values.toList}
    )""".stripMargin
  }
}
