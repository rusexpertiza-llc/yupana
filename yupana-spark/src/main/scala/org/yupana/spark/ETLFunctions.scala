package org.yupana.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{Schema, Table}
import org.yupana.core.TSDB
import org.yupana.hbase.HBaseUtils
import org.yupana.schema.Dimensions

import scala.language.implicitConversions

object ETLFunctions extends StrictLogging {

  def processTransactions(context: EtlContext,
                          schema: Schema,
                          dataPoints: RDD[DataPoint]): Unit = {

    dataPoints.foreachPartition { ls =>
      ls.sliding(5000, 5000).foreach { batch =>
        val dps = batch.toList

        logger.trace(s"Put ${dps.size} datapoints")
        context.tsdb.put(dps)

        if (context.cfg.loadInvertedIndex) {
          val names = dps.flatMap(_.dimensions.get(Dimensions.ITEM_TAG)).toSet
          context.itemsInvertedIndex.putItemNames(names)
        }

        val byTable = dps.groupBy(_.table)

        byTable.foreach { case (t, ps) =>
          if (schema.rollups.exists(_.fromTable.name == t.name)) {
            invalidateRollups(context.tsdb, ps, t)
          }
        }
      }
    }
  }

  def invalidateRollups(tsdb: TSDB, dps: Seq[DataPoint], table: Table): Unit = {
    tsdb.getRollupSpecialField("etl", table).foreach { etlObligatoryRecalc =>

      val rollupStatuses = dps
        .filter(_.time < etlObligatoryRecalc)
        .groupBy(dp => HBaseUtils.baseTime(dp.time, table))
        .mapValues(dps => invalidationMark(dps.head))
        .toSeq

      tsdb.putRollupStatuses(rollupStatuses, table)
    }
  }

  private def invalidationMark(dataPoint: DataPoint): String = {
    dataPoint.dimensions.foldLeft(dataPoint.time.toString) {
      case (acc, (_, v)) => acc + v
    }
  }

  implicit def dStream2Functions(stream: DStream[DataPoint]): DataPointStreamFunctions= new DataPointStreamFunctions(stream)
  implicit def rdd2Functions(rdd: RDD[DataPoint]): DataPointRddFunctions = new DataPointRddFunctions(rdd)
}

class DataPointStreamFunctions(stream: DStream[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlContext, schema: Schema): DStream[DataPoint] = {
    stream.foreachRDD { rdd =>
      ETLFunctions.processTransactions(context, schema, rdd)
    }

    stream
  }
}

class DataPointRddFunctions(rdd: RDD[DataPoint]) extends Serializable {
  def saveDataPoints(context: EtlContext, schema: Schema): Unit = {
    ETLFunctions.processTransactions(context, schema, rdd)
  }
}
