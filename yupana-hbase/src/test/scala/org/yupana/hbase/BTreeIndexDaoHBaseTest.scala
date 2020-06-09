package org.yupana.hbase

import org.scalatest.{ FlatSpecLike, Matchers }

trait BTreeIndexDaoHBaseTest extends HBaseTestBase with FlatSpecLike with Matchers {
  "BTreeIndexDaoHBase" should "put and get values by keys" in {

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
