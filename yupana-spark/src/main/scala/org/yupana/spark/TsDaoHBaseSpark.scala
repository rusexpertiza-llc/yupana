package org.yupana.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, MultiTableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.yupana.api.schema.Table
import org.yupana.core.MapReducible
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase.{HBaseUtils, HdfsFileUtils, TSDOutputRow, TSDaoHBaseBase}

import scala.collection.JavaConverters._

class TsDaoHBaseSpark(@transient val sparkContext: SparkContext,
                      config: Config,
                      override val dictionaryProvider: DictionaryProvider
                     ) extends TSDaoHBaseBase[RDD] with Serializable {

  override val mr: MapReducible[RDD] = new RddMapReducible(sparkContext)

  override def executeScans(table: Table, scans: Seq[Scan], metricCollector: MetricQueryCollector): RDD[TSDOutputRow[Long]] = {
    if (scans.nonEmpty) {
      val tableName = Bytes.toBytes(HBaseUtils.tableNameString(config.hbaseNamespace, table))
      val rdds = scans.sliding(50000, 50000).map { qs =>
        val s = scans.map(_.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName))
        executeScans(table, s)
      }
      sparkContext.union(rdds.toSeq)
    } else {
      sparkContext.emptyRDD[TSDOutputRow[Long]]
    }
  }

  private def executeScans(table: Table, scans: Seq[Scan]): RDD[TSDOutputRow[Long]] = {
    val job: Job = Job.getInstance(TsDaoHBaseSpark.hbaseConfiguration(config))
    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(scans.asJava, classOf[IdentityTableMapper], null, null, job)

    val jconf = new JobConf(job.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jconf)

    val hbaseRdd = sparkContext.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[MultiTableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hbaseRdd.map { case (_, hbaseResult) =>
      HBaseUtils.getTsdRowFromResult(table, hbaseResult)
    }
  }
}

object TsDaoHBaseSpark {
  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.properties)
    }
    configuration
  }
}
