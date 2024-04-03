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

package org.yupana.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.yupana.api.query.DataPoint

import scala.language.implicitConversions

object ETLFunctions extends StrictLogging {

  def processTransactions(context: EtlSparkHBaseContext, dataPoints: RDD[DataPoint]): Unit = {
    dataPoints.foreachPartition({ ls => context.tsdb.put(ls) })
  }

  implicit def dStream2Functions(stream: DStream[DataPoint]): DataPointStreamFunctions =
    new DataPointStreamFunctions(stream)
  implicit def rdd2Functions(rdd: RDD[DataPoint]): DataPointRddFunctions = new DataPointRddFunctions(rdd)
}

class DataPointStreamFunctions(stream: DStream[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlSparkHBaseContext): DStream[DataPoint] = {
    stream.foreachRDD { rdd =>
      ETLFunctions.processTransactions(context, rdd)
    }

    stream
  }
}

class DataPointRddFunctions(rdd: RDD[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlSparkHBaseContext): Unit = {
    ETLFunctions.processTransactions(context, rdd)
  }
}
