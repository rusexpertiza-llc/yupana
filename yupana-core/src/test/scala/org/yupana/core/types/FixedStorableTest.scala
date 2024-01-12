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
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.types.{ FixedStorable, ReaderWriter }
import org.yupana.readerwriter.{ ByteBufferEvalReaderWriter, ID, TypedInt }

import java.nio.ByteBuffer

class FixedStorableTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit val rw: ReaderWriter[ByteBuffer, ID, TypedInt] = ByteBufferEvalReaderWriter

  "FixedStorable" should "handle Long values" in readWriteTest[Long]

  it should "handle Int values" in readWriteTest[Int]

  it should "handle Double values" in readWriteTest[Double]

  private def readWriteTest[T: FixedStorable: Arbitrary] = {
    val fs = implicitly[FixedStorable[T]]
    forAll { v: T =>
      val bb = ByteBuffer.allocate(1000)
      fs.write(bb, v: ID[T])
      bb.rewind()
      fs.read(bb) shouldEqual v
    }
  }
}
