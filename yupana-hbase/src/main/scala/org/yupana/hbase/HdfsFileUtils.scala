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

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataOutputStream, FileStatus, FileSystem, Path }
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.yupana.settings.Settings

import scala.annotation.tailrec

object HdfsFileUtils {

  private def writeData(path: String, hadoopConfiguration: Configuration, delete: Boolean)(
      open: (FileSystem, Path) => FSDataOutputStream
  )(f: DataOutputStream => Unit): Unit = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    if (delete) {
      fs.delete(ppath, true)
    }
    val factory = new CompressionCodecFactory(hadoopConfiguration)
    val codec = factory.getCodec(ppath)
    val stream: DataOutputStream = if (codec != null) {
      new DataOutputStream(codec.createOutputStream(open(fs, ppath)))
    } else {
      open(fs, ppath)
    }
    f(stream)
    stream.close()
    fs.close()
  }

  def isFileExists(path: String, hadoopConfiguration: Configuration): Boolean = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    fs.exists(ppath)
  }

  def saveToHdfsFile(path: String, hadoopConfiguration: Configuration, f: Writer => Unit): Unit = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    fs.delete(ppath, true)
    val o = new OutputStreamWriter(fs.create(ppath), StandardCharsets.UTF_8)
    f(o)
    o.close()
    fs.close()
  }

  def saveDataToHdfs(path: String, hadoopConfiguration: Configuration, f: DataOutputStream => Unit): Unit = {
    writeData(path, hadoopConfiguration, delete = true)((fs, ppath) => fs.create(ppath))(f)
  }

  def appendDataToHdfs(path: String, hadoopConfiguration: Configuration, f: DataOutputStream => Unit): Unit = {
    @tailrec
    def nextTry(n: Int): Boolean = {
      try {
        writeData(path, hadoopConfiguration, delete = false)((fs, ppath) => fs.append(ppath))(f)
        true
      } catch {
        case e: Throwable =>
          if (n > 0) {
            Thread.sleep(500)
            nextTry(n - 1)
          } else {
            throw e
          }
      }
    }
    nextTry(5)
  }

  def readDataFromHdfs[T](path: String, hadoopConfiguration: Configuration, f: DataInputStream => T): T = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    val factory = new CompressionCodecFactory(hadoopConfiguration)
    val codec = factory.getCodec(ppath)
    val stream: DataInputStream = if (codec != null) {
      new DataInputStream(codec.createInputStream(fs.open(ppath)))
    } else {
      fs.open(ppath)
    }
    val result = f(stream)
    result
  }

  def listFiles(path: String, hadoopConfiguration: Configuration): Array[FileStatus] = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    fs.listStatus(ppath)
  }

  def addHdfsPathToConfiguration(configuration: Configuration, settings: Settings): Unit = {
    configuration.set("fs.defaultFS", settings("fs.defaultFS"))
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    configuration.set("dfs.ha.automatic-failover.enabled", "true")

    val cluster = settings[String]("dfs.nameservices")
    configuration.set("dfs.nameservices", cluster)
    configuration.set(s"dfs.ha.namenodes.$cluster", "nn1,nn2")

    configuration.set(s"dfs.namenode.rpc-address.$cluster.nn1", settings(s"dfs.namenode.rpc-address.$cluster.nn1"))
    configuration.set(s"dfs.namenode.rpc-address.$cluster.nn2", settings(s"dfs.namenode.rpc-address.$cluster.nn2"))

    configuration.set(
      s"dfs.client.failover.proxy.provider.$cluster",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
  }
}
