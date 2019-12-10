package org.yupana.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

class BTreeIndexDaoHBaseTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val utility = new HBaseTestingUtility

  override def beforeAll(): Unit = {
    utility.startMiniCluster(1, 1)
  }

  override protected def afterAll(): Unit = {
    utility.shutdownMiniCluster()
  }

  "BTreeIndexDaoHBase" should "put and get values by keys" in {

    val connection = new ExternalLinkHBaseConnection(utility.getConfiguration, "test")
    val dao = new BTreeIndexDaoHBase(
      connection,
      "test",
      Serializers.longSerializer,
      Serializers.longDeserializer,
      Serializers.stringSerializer,
      Serializers.stringDeserializer
    )

    dao.put(1, "Test1")
    dao.put(2, "Test2")
    dao.put(3, "Test3")

    dao.get(Seq(1L, 2)) shouldBe Map(1 -> "Test1", 2 -> "Test2")
    dao.get(Seq(1L, 3)) shouldBe Map(1 -> "Test1", 3 -> "Test3")

    dao.get(1) shouldBe Some("Test1")
    dao.get(3) shouldBe Some("Test3")
  }

}
