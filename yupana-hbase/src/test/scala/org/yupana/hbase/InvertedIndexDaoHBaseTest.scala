package org.yupana.hbase

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait InvertedIndexDaoHBaseTest extends HBaseTestBase with AnyFlatSpecLike with Matchers {
  "InvertedIndexDaoHBase" should "put and get values" in {
    val dao = new InvertedIndexDaoHBase(
      connection,
      "test",
      Serializers.stringSerializer,
      Serializers.stringDeserializer,
      Serializers.intSerializer,
      Serializers.intDeserializer
    )

    dao.put("foo", Set(1, 2, 3, 5))
    dao.put("bar", Set(4, 6))

    dao.values("foo").toSet shouldEqual Set(1, 2, 3, 5)
    dao.values("bar").toSet shouldEqual Set(4, 6)
    dao.values("baz") shouldBe empty
  }

  it should "support batch put and get" in {
    val dao = new InvertedIndexDaoHBase(
      connection,
      "test_batch",
      Serializers.stringSerializer,
      Serializers.stringDeserializer,
      Serializers.intSerializer,
      Serializers.intDeserializer
    )

    dao.batchPut(Map("foo" -> Set(1, 2, 3, 5), "bar" -> Set(4, 5, 6)))

    dao.values("foo").toSet shouldEqual Set(1, 2, 3, 5)
    dao.allValues(Set("bar", "foo")).toSet shouldEqual Set(1, 2, 3, 4, 5, 6)
  }

  it should "support get values by prefix" in {
    val dao = new InvertedIndexDaoHBase(
      connection,
      "test_prefix",
      Serializers.stringSerializer,
      Serializers.stringDeserializer,
      Serializers.intSerializer,
      Serializers.intDeserializer
    )

    dao.batchPut(Map("a" -> Set(1, 2), "aa" -> Set(3, 4), "aaa" -> Set(4, 5, 6)))
    dao.valuesByPrefix("aa").toSet shouldEqual Set(3, 4, 5, 6)
  }
}
