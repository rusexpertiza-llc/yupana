package org.yupana.core.utils.metric

import java.util.Timer

import org.yupana.core.dao.TsdbQueryMetricsDao

class QueryCollectorContext(val metricsDao: () => TsdbQueryMetricsDao,
                            val operationName: String,
                            val metricsUpdateInterval: Int,
                            val sparkQuery: Boolean = false
                           ) extends Serializable {

  @transient lazy val timer = new Timer()

  var queryActive: Boolean = true
}