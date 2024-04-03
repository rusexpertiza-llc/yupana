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

package org.yupana.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.yupana.core.TsdbConfig
import org.yupana.settings.Settings

class TSDBHBaseConfig(
    override val collectMetrics: Boolean,
    override val metricsUpdateInterval: Int,
    override val extractBatchSize: Int,
    override val putBatchSize: Int,
    override val putEnabled: Boolean,
    override val maxRegions: Int,
    override val reduceLimit: Int,
    override val needCheckSchema: Boolean,
    override val compression: String,
    val hbaseNamespace: String,
    val hbaseZookeeper: String,
    val hbaseWriteBufferSize: Option[Long],
    val settings: Settings
) extends TsdbConfig with Serializable {

  def hBaseConfiguration: Configuration = {
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", hbaseZookeeper)
    hbaseconf.set("zookeeper.session.timeout", 180000.toString)
    hbaseWriteBufferSize.foreach(x => hbaseconf.set("hbase.client.write.buffer", x.toString))
    hbaseconf
  }
}

case class SimpleTSDBHBaseConfig(
    override val hbaseNamespace: String,
    override val collectMetrics: Boolean = false,
    override val metricsUpdateInterval: Int = 30000,
    override val extractBatchSize: Int = 10000,
    override val putBatchSize: Int = 1000,
    override val putEnabled: Boolean = false,
    override val maxRegions: Int = 50,
    override val reduceLimit: Int = Int.MaxValue,
    override val needCheckSchema: Boolean = true,
    override val compression: String = "snappy",
    override val hbaseZookeeper: String,
    override val hbaseWriteBufferSize: Option[Long] = None,
    override val settings: Settings
) extends TSDBHBaseConfig(
      collectMetrics,
      metricsUpdateInterval,
      extractBatchSize,
      putBatchSize,
      putEnabled,
      maxRegions,
      reduceLimit,
      needCheckSchema,
      compression,
      hbaseNamespace,
      hbaseZookeeper,
      hbaseWriteBufferSize,
      settings
    )
