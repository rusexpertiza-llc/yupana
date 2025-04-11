package org.yupana.core.model

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class BitSetOpsTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "BitSet" should "perform set, check and get bits" in {

    val a = Array.ofDim[Long](100)
    val numOfBits = 100 * 64
    val genBit = Gen.choose(0, numOfBits - 1)

    forAll(genBit) { bit: Int =>
      whenever(bit > 0 && bit < numOfBits) {

        BitSetOps.set(a, bit)
        BitSetOps.check(a, bit) shouldBe true
        for {
          unsetBit <- 0 until numOfBits if unsetBit != bit
        } {
          BitSetOps.check(a, unsetBit) shouldBe false
        }
        BitSetOps.clear(a, bit)

        BitSetOps.check(a, bit) shouldBe false

        for { unsetBit <- 0 until numOfBits } {
          BitSetOps.check(a, unsetBit) shouldBe false
        }

        for { setBit <- 0 until numOfBits if setBit != bit } {

          BitSetOps.set(a, setBit)
        }

        BitSetOps.check(a, bit) shouldBe false

        for { setBit <- 0 until numOfBits if setBit != bit } {
          BitSetOps.check(a, setBit) shouldBe true
        }

        for { setBit <- 0 until numOfBits if setBit != bit } {
          BitSetOps.clear(a, setBit)
        }

        for { setBit <- 0 until numOfBits } {
          BitSetOps.check(a, setBit)
        }
      }
    }
  }
}
