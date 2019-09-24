package org.yupana.hbase

case class TSDInputRow[T](
  key: TSDRowKey[T],
  values: TSDRowValues
) {
  override def toString: String = {
    s"""TSDRow(
       | key: ${key.baseTime} / ${key.dimIds.toList}
       | values: $values
    )""".stripMargin
  }
}
