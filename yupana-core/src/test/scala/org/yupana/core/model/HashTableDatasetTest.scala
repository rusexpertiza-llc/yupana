package org.yupana.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.query.Expression
import org.yupana.api.query.syntax.All.const
import org.yupana.serialization.MemoryBuffer

class HashTableDatasetTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  val intExpr = const(10)
  val doubleExpr = const(10.0)
  val stringExpr = const("11111")
  val refExpr = const(0.toByte)

  val valExprIndex: Map[Expression[_], Int] = Map(intExpr -> 0, doubleExpr -> 2)
  val refExprIndex: Map[Expression[_], Int] = Map(refExpr -> 1, stringExpr -> 3)

  "HashTableDataset" should "set and get values for the same key" in {

    val schema = new DatasetSchema(valExprIndex, refExprIndex, None)
    val ds = new HashTableDataset(schema)

    val key = testKey(10)
    ds.set(key, intExpr, 1)
    ds.setRef(key, refExpr, "Ref")
    ds.set(key, doubleExpr, 2.0)
    ds.set(key, stringExpr, "text")

    ds.get(key, intExpr) shouldEqual 1
    ds.getRef(key, refExpr) shouldEqual "Ref"
    ds.get(key, doubleExpr) shouldEqual 2.0
    ds.get(key, stringExpr) shouldEqual "text"
    ds.isDefined(key, intExpr) shouldEqual true
    ds.isDefined(key, refExpr) shouldEqual true
    ds.isDefined(key, doubleExpr) shouldEqual true
    ds.isDefined(key, stringExpr) shouldEqual true
  }

  it should "set and get values of different types in several rows" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, None)
    val ds = new HashTableDataset(schema)

    val key0 = testKey(0)
    ds.set(key0, intExpr, 1)
    ds.setRef(key0, refExpr, "Ref1")
    ds.set(key0, doubleExpr, 1.0)
    ds.set(key0, stringExpr, "text1")

    val key1 = testKey(1)
    ds.set(key1, intExpr, 2)
    ds.set(key1, doubleExpr, 2.0)
    ds.set(key1, stringExpr, "text2")

    val key3 = testKey(3)
    ds.set(key3, intExpr, 3)
    ds.set(key3, doubleExpr, 3.0)
    ds.set(key3, stringExpr, "text3")

    val key4 = testKey(4)
    ds.set(key4, intExpr, 5)
    ds.set(key4, stringExpr, "text5")
    ds.setRef(key4, refExpr, "Ref5")

    ds.get(key0, intExpr) shouldEqual 1
    ds.get(key0, doubleExpr) shouldEqual 1.0
    ds.get(key0, stringExpr) shouldEqual "text1"

    ds.get(key1, intExpr) shouldEqual 2
    ds.get(key1, doubleExpr) shouldEqual 2.0
    ds.get(key1, stringExpr) shouldEqual "text2"

    ds.contains(testKey(2)) shouldEqual false

    ds.get(key3, intExpr) shouldEqual 3
    ds.get(key3, doubleExpr) shouldEqual 3.0
    ds.get(key3, stringExpr) shouldEqual "text3"

    ds.get(key4, intExpr) shouldEqual 5
    ds.isDefined(key4, intExpr) shouldEqual true
    ds.getRef(key4, refExpr) shouldEqual "Ref5"
    ds.isDefined(key4, refExpr) shouldEqual true
    ds.isNull(key4, doubleExpr) shouldBe true
    ds.isDefined(key4, doubleExpr) shouldBe false
    ds.get(key4, stringExpr) shouldEqual "text5"
  }

  it should "operate on multiple batches" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, None)
    val ds = new HashTableDataset(schema)

    val range = 0 to BatchDataset.MAX_MUM_OF_ROWS * 10

    range.foreach { i =>
      val key = testKey(i)
      ds.set(key, intExpr, i)
    }

    range.foreach { i =>
      val key = testKey(i)
      ds.get(key, intExpr) shouldEqual i
    }
  }

  it should "partition" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, None)
    val ds = new HashTableDataset(schema)

    val key: AnyRef = "1"
    ds.set(key, intExpr, 1)
    ds.set(key, doubleExpr, 2.0d)
    ds.set(key, stringExpr, "3")
    ds.set(key, refExpr, 4.toByte)

    val r = ds.partition(1)

    val batch = r.toMap.apply(0).apply(0)
    batch.get(0, intExpr) shouldEqual 1
    batch.get(0, doubleExpr) shouldEqual 2.0
    batch.get(0, stringExpr) shouldEqual "3"
    batch.get(0, refExpr) shouldEqual 4.toByte
  }

  def testKey(v: Long): MemoryBuffer = {
    val buf = MemoryBuffer.allocateHeap(8)
    buf.putLong(v)
    buf
  }

}
