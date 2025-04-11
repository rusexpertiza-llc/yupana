/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.yupana.khipu
//
//import org.yupana.api.query.Expression
//import org.yupana.api.schema.{ Dimension, Table }
//import org.yupana.core.model.InternalQuery
//import org.yupana.core.utils.metric.MetricQueryCollector
//
//import scala.collection.mutable
//
//case class InternalQueryContext(
//    table: Table,
//    exprsIndexSeq: Seq[(Expression[_], Int)],
//    dimIndexMap: mutable.Map[Dimension, Int],
//    metricsCollector: MetricQueryCollector
//)
//
//object InternalQueryContext {
//  def apply(query: InternalQuery, metricCollector: MetricQueryCollector): InternalQueryContext = {
//    val dimIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)
//
//    val exprsIndexSeq = query.exprs.toSeq.zipWithIndex
//
//    new InternalQueryContext(query.table, exprsIndexSeq, dimIndexMap, metricCollector)
//  }
//}
