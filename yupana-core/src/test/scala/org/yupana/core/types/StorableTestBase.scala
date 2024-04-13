/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.types

import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.types.{ ID, ReaderWriter, Storable, TypedInt }
import org.yupana.api.{ Blob, Time }

trait StorableTestBase
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  trait BufUtils[B] {
    def createBuffer(size: Int): B
    def position(buf: B): Int
    def rewind(bb: B): Unit
  }

  implicit private val genTime: Arbitrary[Time] = Arbitrary(Arbitrary.arbitrary[Long].map(Time.apply))

  implicit private val genBlob: Arbitrary[Blob] = Arbitrary(Arbitrary.arbitrary[Array[Byte]].map(Blob.apply))

  def storableTest[B](readerWriter: ReaderWriter[B, ID, TypedInt], bufUtils: BufUtils[B]): Unit = {

    implicit val rw: ReaderWriter[B, ID, TypedInt] = readerWriter

    "Serialization" should "preserve doubles on write read cycle" in readWriteTest[Double]

    it should "preserve Bytes on write read cycle" in readWriteTest[Byte]

    it should "preserve Shorts on write read cycle" in readWriteTest[Short]

    it should "preserve Ints on write read cycle" in readWriteTest[Int]

    it should "preserve Longs on write read cycle" in readWriteTest[Long]

    it should "preserve BigDecimals on read write cycle" in readWriteTest[BigDecimal]

    it should "preserve Strings on read write cycle" in readWriteTest[String]

    it should "preserve Time on read write cycle" in readWriteTest[Time]

    it should "preserve Booleans on readwrite cycle" in readWriteTest[Boolean]

    it should "preserve sequences of Int on read write cycle" in readWriteTest[Seq[Int]]

    it should "preserve sequences of String on read write cycle" in readWriteTest[Seq[String]]

    it should "preserve BLOBs on read write cycle" in readWriteTest[Blob]

    def readWriteTest[T: Storable: Arbitrary] = {
      val storable = implicitly[Storable[T]]

      forAll { t: T =>
        val bb = bufUtils.createBuffer(65535)
        val posBeforeWrite = bufUtils.position(bb)
        val actualSize = storable.write(bb, t: ID[T])
        val posAfterWrite = bufUtils.position(bb)
        val expectedSize = posAfterWrite - posBeforeWrite
        expectedSize shouldEqual actualSize
        bufUtils.rewind(bb)
        storable.read(bb) shouldEqual t

        storable.write(bb, 1000, t: ID[T]) shouldEqual expectedSize
        storable.read(bb, 1000) shouldEqual t
      }
    }
  }

  def compactTest[B](readerWriter: ReaderWriter[B, ID, TypedInt], bufUtils: BufUtils[B]): Unit = {
    implicit val rw: ReaderWriter[B, ID, TypedInt] = readerWriter

    it should "compact numbers" in {
      val storable = implicitly[Storable[Long]]

      val table = Table(
        ("Value", "Bytes count"),
        (0L, 1),
        (100L, 1),
        (-105L, 1),
        (181L, 2),
        (-222L, 2),
        (1000L, 3),
        (-1000L, 3),
        (70000L, 4),
        (-70000L, 4),
        (3000000000L, 5),
        (-1099511627776L, 6),
        (1099511627776L, 7),
        (290000000000000L, 8),
        (-5000000000000000000L, 9),
        (1000000000000000000L, 9)
      )

      forAll(table) { (x, len) =>
        val bb = bufUtils.createBuffer(9)
        storable.write(bb, x: ID[Long])
        bufUtils.position(bb) shouldEqual len
      }
    }

    it should "not read Long as Int if it overflows" in {
      val longStorable = implicitly[Storable[Long]]
      val intStorable = implicitly[Storable[Int]]

      val bb = bufUtils.createBuffer(9)

      longStorable.write(bb, 3000000000L: ID[Long])
      bufUtils.rewind(bb)
      an[IllegalArgumentException] should be thrownBy intStorable.read(bb)
    }
  }
}
