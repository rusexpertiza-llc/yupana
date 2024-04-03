package org.yupana.etl

import org.yupana.hbase.TSDBHBaseConfig

case class EtlHbaseConfig(tsdbConfig: TSDBHBaseConfig, putIntoInvertedIndex: Boolean)
