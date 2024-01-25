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

package org.yupana.khipu.examples

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, MetricValue }
import org.yupana.core.SimpleTsdbConfig
import org.yupana.khipu.TSDBKhipu

import java.time.{ LocalDateTime, ZoneOffset }

object Generator extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val schema = ExampleSchema.schema

    val config = Config.create(ConfigFactory.load())
    val tsdbConfig = SimpleTsdbConfig(collectMetrics = true, putEnabled = true)

    val changelogDao = new DummyChangelogDao

    val tsdb = TSDBKhipu(schema, identity, tsdbConfig, config.settings, changelogDao)

    val N_CATS = 4
    val N_BRANDS = 100
    val N_DEVICES = 100
    val N_OPS = 1
    val N_POS = 4

    val total = N_CATS * N_BRANDS * N_DEVICES * N_OPS * N_POS
    logger.info(
      s"Start generation $total records ($N_CATS cats," +
        s" $N_BRANDS brands, $N_DEVICES devices, $N_OPS operations, $N_POS positions ) "
    )

    val startTime = LocalDateTime.of(2020, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    val endTime = LocalDateTime.of(2021, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli

    var i = 0L

    val records = for {
      cat <- (1 to N_CATS).iterator
      brand <- (1 to N_BRANDS).iterator
      device <- (1 to N_DEVICES).iterator
      op <- (1 to N_OPS).iterator
      pos <- (1 to N_POS).iterator
    } yield {

      i += 1

      if (i % 1000000 == 0) logger.info(s"$i (${i * 100 / total} %) records added")

      val time = startTime + i * (endTime - startTime) / total

      val dims: Map[Dimension, Any] = Map(
        ExampleSchema.CAT -> cat.toLong,
        ExampleSchema.BRAND -> brand,
        ExampleSchema.DEVICE -> device,
        ExampleSchema.OPERATION -> op.toByte,
        ExampleSchema.POS -> pos.toByte
      )

      val metrics = Seq(
        MetricValue(ExampleSchema.SUM, 1000),
        MetricValue(ExampleSchema.QUANTITY, 2)
      )

      DataPoint(ExampleSchema.salesTable, time, dims, metrics)
    }

    records.grouped(50000).foreach(group => tsdb.put(group.iterator))

  }

}
