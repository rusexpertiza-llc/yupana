package org.yupana.api

import org.scalatest.{ FlatSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class HexStringTest extends FlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "HexString" should "represent bytes as string" in {
    HexString(Array[Byte](1, 16, 127)).hex shouldEqual "01107f"
    HexString(Array[Byte](32, 248.toByte, 5)).hex shouldEqual "20f805"
  }

  it should "convert back and forth between string and array" in {
    forAll { a: Array[Byte] =>
      a shouldEqual HexString.stringToBytes(HexString.bytesToString(a))
    }
  }

}
