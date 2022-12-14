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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.Partition
import org.yupana.core.settings.Settings
import org.yupana.hbase.HdfsFileUtils

import scala.io.Source
import scala.util.Using

class HDFSProgressSaver[P <: Partition](
    fileName: String,
    partitionStorable: PartitionStorable[P],
    settings: Settings
) extends ProgressSaver[P]
    with Serializable {

  private val allPartitionsHeader = "** All Partitions **"
  private val completedPartitionsHeader = "** Completed partitions **"

  override def writePartitions(ps: Seq[P]): Unit = {
    val hBaseConfiguration = createHBaseConfiguration()
    HdfsFileUtils.saveDataToHdfs(
      fileName,
      hBaseConfiguration,
      os => {
        os.writeBytes(allPartitionsHeader + "\n")
        ps.foreach { partition =>
          os.writeBytes(partitionStorable.asString(partition) + "\n")
        }
        os.writeBytes(completedPartitionsHeader + "\n")
      }
    )
  }

  override def writeProgress(p: P): Unit = {
    val hBaseConfiguration = createHBaseConfiguration()
    HdfsFileUtils.appendDataToHdfs(
      fileName,
      hBaseConfiguration,
      os => os.writeBytes(partitionStorable.asString(p) + "\n")
    )
  }

  override def readPartitions: Seq[P] = {
    val hBaseConfiguration = createHBaseConfiguration()

    if (HdfsFileUtils.isFileExists(fileName, hBaseConfiguration)) {
      val lines = HdfsFileUtils.readDataFromHdfs[List[String]](
        fileName,
        hBaseConfiguration,
        is => {
          Using.resource(Source.fromInputStream(is)) { s =>
            s.getLines().toList
          }
        }
      )
      if (!lines.headOption.contains(allPartitionsHeader)) {
        throw new IllegalArgumentException("Incorrect format of partitions file")
      }
      val (allPartitions, tail) = lines.tail.span(_ != completedPartitionsHeader)
      val donePartitions = tail.drop(1).toSet
      val remains = allPartitions
        .filterNot(p => donePartitions.contains(p))
        .zipWithIndex
        .map {
          case (line, index) => partitionStorable.fromString(line, index)
        }
      remains
    } else Seq.empty
  }

  private def createHBaseConfiguration(): Configuration = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set("hbase.zookeeper.quorum", settings("hbase.zookeeper"))
    hBaseConfiguration.set("zookeeper.session.timeout", "180000")
    HdfsFileUtils.addHdfsPathToConfiguration(hBaseConfiguration, settings)
    hBaseConfiguration
  }
}
