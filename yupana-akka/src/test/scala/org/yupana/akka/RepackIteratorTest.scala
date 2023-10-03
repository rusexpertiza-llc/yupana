package org.yupana.akka

import akka.util.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RepackIteratorTest extends AnyFlatSpec with Matchers {

  "RepackIterator" should "iterate over one input ByteString" in {

    val it = Iterator(ByteString("test"))

    it.hasNext shouldBe true

    val rit = new RepackIterator(it, 32768)

    rit.hasNext shouldBe true
    rit.toList shouldBe List(ByteString("test"))

  }

  it should "iterate over a multiple small input ByteStrings" in {

    val it = Iterator(ByteString("1"), ByteString("2"), ByteString("3"))

    it.hasNext shouldBe true

    val rit = new RepackIterator(it, 32768)

    rit.hasNext shouldBe true
    rit.toList shouldBe List(ByteString("123"))

  }

  it should "iterate over a lot of small input ByteStrings" in {

    val src = (1 to 1000000).map(_.toString)
    val expected = src.mkString("").grouped(32768).map(ByteString(_)).toList
    val it = src.map(ByteString(_)).iterator

    it.hasNext shouldBe true

    val rit = new RepackIterator(it, 32768)

    rit.hasNext shouldBe true
    val res = rit.toList

    expected.size shouldBe res.size
    expected.map(_.size) shouldBe res.map(_.size)

    res shouldBe expected
  }

  it should "iterate over a one big input ByteString" in {

    val src = (1 to 1000000).map(_.toString)
    val expected = src.mkString("").grouped(32768).map(ByteString(_)).toList
    val it = Iterator(ByteString(src.mkString("")))

    it.hasNext shouldBe true

    val rit = new RepackIterator(it, 32768)

    rit.hasNext shouldBe true
    val res = rit.toList

    expected.size shouldBe res.size
    expected.map(_.size) shouldBe res.map(_.size)

    res shouldBe expected
  }

  it should "iterate over a multiple big input ByteString" in {

    val t = (1 to 100000).map(_.toString).mkString("")
    val src = (1 to 20).map(_ => t)
    val expected = src.mkString("").grouped(32768).map(ByteString(_)).toList
    val it = src.map(ByteString(_)).iterator

    it.hasNext shouldBe true

    val rit = new RepackIterator(it, 32768)

    rit.hasNext shouldBe true
    val res = rit.toList

    expected.size shouldBe res.size
    expected.map(_.size) shouldBe res.map(_.size)

    res shouldBe expected
  }

}
