package org.yupana.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.query.Expression
import org.yupana.api.query.syntax.All.const
import org.scalacheck.{ Arbitrary, Gen }

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
class BatchDatasetTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  val intExpr = const(10)
  val doubleExpr = const(10.0)
  val stringExpr = const("11111")
  val refExpr = const(0.toByte)

  val valExprIndex: Map[Expression[_], Int] = Map(intExpr -> 0, doubleExpr -> 2)
  val refExprIndex: Map[Expression[_], Int] = Map(refExpr -> 1, stringExpr -> 3)
  val nameMapping: Map[String, Int] = Map(
    "intField" -> 0,
    "ref" -> 1,
    "doubleField" -> 2,
    "stringField" -> 3
  )

  "BatchDataset" should "set and get values of different types for the same row" in {

    val schema = new DatasetSchema(valExprIndex, refExprIndex, nameMapping, None)
    val batch = new BatchDataset(schema)

    batch.set(0, intExpr, 1)
    batch.setRef(0, refExpr, "Ref")
    batch.set(0, doubleExpr, 2.0)
    batch.set(0, stringExpr, "text")

    batch.get(0, intExpr) shouldEqual 1
    batch.getRef(0, refExpr) shouldEqual "Ref"
    batch.get(0, doubleExpr) shouldEqual 2.0
    batch.get(0, stringExpr) shouldEqual "text"
    batch.isDefined(0, intExpr) shouldEqual true
    batch.isDefined(0, refExpr) shouldEqual true
    batch.isDefined(0, doubleExpr) shouldEqual true
    batch.isDefined(0, stringExpr) shouldEqual true

    batch.get[Int](0, "intField") shouldEqual 1
    batch.get[Double](0, "doubleField") shouldEqual 2.0
    batch.get[String](0, "stringField") shouldEqual "text"
  }

  it should "set and get values of different types in several rows" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, Map.empty, None)
    val batch = new BatchDataset(schema)

    batch.set(0, intExpr, 1)
    batch.setRef(0, refExpr, "Ref1")
    batch.set(0, doubleExpr, 1.0)
    batch.set(0, stringExpr, "text1")

    batch.set(1, intExpr, 2)
    batch.set(1, doubleExpr, 2.0)
    batch.set(1, stringExpr, "text2")

    batch.set(3, intExpr, 3)
    batch.set(3, doubleExpr, 3.0)
    batch.set(3, stringExpr, "text3")

    batch.set(4, intExpr, 5)
    batch.set(4, stringExpr, "text5")
    batch.setRef(4, refExpr, "Ref5")

    batch.get(0, intExpr) shouldEqual 1
    batch.get(0, doubleExpr) shouldEqual 1.0
    batch.get(0, stringExpr) shouldEqual "text1"

    batch.get(1, intExpr) shouldEqual 2
    batch.get(1, doubleExpr) shouldEqual 2.0
    batch.get(1, stringExpr) shouldEqual "text2"

    batch.isDefined(2, intExpr) shouldEqual false
    batch.isDefined(2, doubleExpr) shouldEqual false
    batch.isDefined(2, stringExpr) shouldEqual false

    batch.isNull(2, intExpr) shouldEqual true
    batch.isNull(2, doubleExpr) shouldEqual true
    batch.isNull(2, stringExpr) shouldEqual true

    batch.get(3, intExpr) shouldEqual 3
    batch.get(3, doubleExpr) shouldEqual 3.0
    batch.get(3, stringExpr) shouldEqual "text3"

    batch.get(4, intExpr) shouldEqual 5
    batch.isDefined(4, intExpr) shouldEqual true
    batch.getRef(4, refExpr) shouldEqual "Ref5"
    batch.isDefined(4, refExpr) shouldEqual true
    batch.isNull(4, doubleExpr) shouldBe true
    batch.isDefined(4, doubleExpr) shouldBe false
    batch.get(4, stringExpr) shouldEqual "text5"
  }

  it should "set and get fixed length values" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, Map.empty, None)
    val batch = new BatchDataset(schema)

    val rowNum = for (n <- Gen.choose(0, BatchDataset.MAX_MUM_OF_ROWS)) yield n
    val ints = Arbitrary.arbitrary[Int]

    forAll(rowNum, ints) {
      case (r, i) =>
        batch.set(r, intExpr, i)
        batch.get(r, intExpr) shouldEqual i
    }
  }

  it should "set and get variable length values" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, Map.empty, None)
    val batch = new BatchDataset(schema)

    val rowNum = Gen.choose(0, BatchDataset.MAX_MUM_OF_ROWS - 1)
    val strings = Arbitrary.arbitrary[String]

    forAll(rowNum, strings) {
      case (r, s) =>
        whenever(r >= 0 && r < BatchDataset.MAX_MUM_OF_ROWS) {
          batch.set(r, stringExpr, s)
          batch.get(r, stringExpr) shouldEqual s
          batch.isDefined(r, stringExpr) shouldEqual true
        }
    }
  }

  it should "serialize/deserialize" in {
    val schema = new DatasetSchema(valExprIndex, refExprIndex, Map.empty, None)
    val batch = new BatchDataset(schema)
    batch.set(0, intExpr, 1)
    batch.set(0, doubleExpr, 2.0d)
    batch.set(0, stringExpr, "3")
    batch.set(0, refExpr, 1.toByte)

    val bf = new ByteArrayOutputStream(1000000)
    val oos = new ObjectOutputStream(bf)
    oos.writeObject(batch)

    val is = new ByteArrayInputStream(bf.toByteArray)
    val ois = new ObjectInputStream(is)
    val deserializedBatch = ois.readObject().asInstanceOf[BatchDataset]

    deserializedBatch.isDefined(0, intExpr) shouldBe true
    deserializedBatch.get(0, intExpr) shouldEqual 1
    deserializedBatch.get(0, doubleExpr) shouldEqual 2.0
    deserializedBatch.get(0, stringExpr) shouldEqual "3"
    deserializedBatch.get(0, refExpr) shouldEqual 1.toByte
  }
}
