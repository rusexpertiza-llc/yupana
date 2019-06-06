package org.yupana.hbase

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory

object HdfsFileUtils {

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
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    fs.delete(ppath, true)
    val factory = new CompressionCodecFactory(hadoopConfiguration)
    val codec = factory.getCodec(ppath)
    val stream: DataOutputStream = if (codec != null) {
      new DataOutputStream(codec.createOutputStream(fs.create(ppath)))
    } else {
      fs.create(ppath)
    }
    f(stream)
    stream.close()
    fs.close()
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
    //    stream.close()
    //    fs.close()
    result
  }

  def listFiles(path: String, hadoopConfiguration: Configuration): Array[FileStatus] = {
    val uri: URI = new URI(path)
    val ppath = new Path(uri)
    val fs = FileSystem.get(hadoopConfiguration)
    fs.listStatus(ppath)
  }

  def addHdfsPathToConfiguration(configuration: Configuration, properties: Properties): Unit = {
    import scala.collection.JavaConverters._
    configuration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    properties.asScala.foreach { case (key, value) =>
      if (key.startsWith("dfs.")) configuration.set(key, value)
    }
  }
}
