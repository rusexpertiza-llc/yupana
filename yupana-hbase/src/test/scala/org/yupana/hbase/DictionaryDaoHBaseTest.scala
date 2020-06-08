package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.scalatest.{ FlatSpecLike, Matchers, OptionValues }
import org.yupana.api.schema.DictionaryDimension

trait DictionaryDaoHBaseTest extends HBaseTestBase with FlatSpecLike with Matchers with OptionValues {
  private lazy val hbaseConnection = ConnectionFactory.createConnection(getConfiguration)

  private val dimension = DictionaryDimension("X")

  "DictionaryDaoHBase" should "generate ids" in {
    val dao = new DictionaryDaoHBase(hbaseConnection, "test")

    dao.createSeqId(dimension) shouldEqual 1
    dao.createSeqId(dimension) shouldEqual 2
    dao.createSeqId(dimension) shouldEqual 3
  }

  it should "get and put" in {
    val dao = new DictionaryDaoHBase(hbaseConnection, "test")

    dao.checkAndPut(dimension, 10L, "Test me")
    dao.checkAndPut(dimension, 20L, "Value")

    dao.getIdByValue(dimension, "Test me").value shouldEqual 10L
    dao.getIdByValue(dimension, "Unknown") shouldBe empty

    dao.getIdsByValues(dimension, Set("Test me", "Value", "One more")) shouldEqual Map("Test me" -> 10L, "Value" -> 20L)
  }
}
