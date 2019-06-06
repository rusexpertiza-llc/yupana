package org.yupana.hbase

object Filtration {
  type TimeFilter = Long => Boolean
  type RowFilter = TSDOutputRow[Long] => Boolean
}
