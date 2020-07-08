package org.yupana.api

import org.scalatest.{ FlatSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class HexStringTest extends FlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "HexString" should "represent bytes as string" in {
    HexString(Array[Byte](1, 16, 127)).hex shouldEqual "01107f"
    HexString(Array[Byte](32, 248.toByte, 5)).hex shouldEqual "20f805"
  }

  it should "construct from strings" in {
    val deadBeef = HexString("deadBeef")
    deadBeef.bytes shouldEqual Array(0xde.toByte, 0xad.toByte, 0xbe.toByte, 0xef.toByte)
    deadBeef.hex shouldEqual "deadbeef"

    val oneTwoThree = HexString("123")
    oneTwoThree.bytes shouldEqual Array(0x01.toByte, 0x23.toByte)
    oneTwoThree.hex shouldEqual "0123"
  }

  it should "convert back and forth between string and array" in {
    forAll { a: Array[Byte] =>
      a shouldEqual HexString.stringToBytes(HexString.bytesToString(a))
    }
  }

}
