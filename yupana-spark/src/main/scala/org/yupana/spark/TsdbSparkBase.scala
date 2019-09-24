package org.yupana.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.client.{ConnectionFactory, Mutation, Result => HBaseResult}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.yupana.api.query.{DataPoint, Query}
import org.yupana.api.schema.{Schema, Table}
import org.yupana.core.dao.{DictionaryProvider, TSReadingDao}
import org.yupana.core.model.{InternalRow, KeyData}
import org.yupana.core.utils.metric.{MetricQueryCollector, NoMetricCollector}
import org.yupana.core.{MapReducible, QueryContext, TsdbBase}
import org.yupana.hbase.{DictionaryDaoHBase, HBaseUtils}

abstract class TsdbSparkBase(@transient val sparkContext: SparkContext,
                override val prepareQuery: Query => Query,
                conf: Config,
                schema: Schema
               ) extends TsdbBase with StrictLogging with Serializable {

  override type Collection[X] = RDD[X]
  override type Result = DataRowRDD

  override val extractBatchSize: Int = conf.extractBatchSize

  HBaseUtils.initStorage(ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(conf)), conf.hbaseNamespace, schema)

  override val mr: MapReducible[RDD] = new RddMapReducible(sparkContext)

  override val dictionaryProvider: DictionaryProvider = new SparkDictionaryProvider(conf)

  override val dao: TSReadingDao[RDD, Long] = new TsDaoHBaseSpark(sparkContext, conf, dictionaryProvider)

  override def createMetricCollector(query: Query): MetricQueryCollector = NoMetricCollector

  override def finalizeQuery(queryContext: QueryContext, data: RDD[Array[Option[Any]]], metricCollector: MetricQueryCollector): DataRowRDD = {
    new DataRowRDD(data, queryContext)
  }

  /**
    * Save DataPoints into table.
    *
    * @note This method takes table as a parameter, and saves only data points related to this table. All data points
    *       related to another tables are ignored.
    *
    * @param dataPointsRDD data points to be saved
    * @param table table to store data points
    */
  def writeRDD(dataPointsRDD: RDD[DataPoint], table: Table): Unit = {
    val hbaseConf = TsDaoHBaseSpark.hbaseConfiguration(conf)

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, HBaseUtils.tableNameString(conf.hbaseNamespace, table))
    hbaseConf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]], classOf[OutputFormat[String, Mutation]])
    hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val job: Job = Job.getInstance(hbaseConf, "TsdbRollup-write")
    TableMapReduceUtil.initCredentials(job)

    val jconf = new JobConf(job.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jconf)

    val filtered = dataPointsRDD.filter(_.table == table)

    val puts = filtered.mapPartitions { partition =>
      partition.grouped(10000).flatMap { dataPoints =>
        val rowsByTable = HBaseUtils.createTsdRows(dataPoints, dictionaryProvider)
        rowsByTable.flatMap { case (_, rows) =>
          rows.map(row => new ImmutableBytesWritable() -> HBaseUtils.createPutOperation(row))
        }
      }
    }
    puts.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def dictionaryRdd(namespace: String, name: String): RDD[(Long, String)] = {
    val scan = DictionaryDaoHBase.getReverseScan

    val job: Job = Job.getInstance(TsDaoHBaseSpark.hbaseConfiguration(conf))
    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(DictionaryDaoHBase.getTableName(namespace, name), scan,
      classOf[IdentityTableMapper], null, null, job)

    val jconf = new JobConf(job.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jconf)

    val hbaseRdd = sparkContext.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[HBaseResult])

    val rowsRdd = hbaseRdd.flatMap { case (_, hbaseResult) =>
      DictionaryDaoHBase.getReversePairFromResult(hbaseResult)
    }
    rowsRdd
  }

  def union(rdds: Seq[DataRowRDD]): DataRowRDD = {
    val rdd = sparkContext.union(rdds.map(_.rows))
    new DataRowRDD(rdd, rdds.head.queryContext)
  }

  override def applyWindowFunctions(queryContext: QueryContext, keysAndValues: RDD[(KeyData, InternalRow)]): RDD[(KeyData, InternalRow)] = {
    throw new UnsupportedOperationException("Window functions are not supported in TSDB Spark")
  }
}
