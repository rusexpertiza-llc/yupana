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

package org.yupana.api

case class HexString(hex: String) {
  def toBytes: Array[Byte] = HexString.stringToBytes(hex)
}

object HexString {
  private val digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  def apply(bytes: Array[Byte]): HexString = {
    new HexString(bytesToString(bytes))
  }

  def bytesToString(bytes: Array[Byte]): String = {
    val l = bytes.length
    val out = new Array[Char](l << 1)

    bytes.indices.foreach { i =>
      out(i * 2) = digits((0xf0 & bytes(i)) >>> 4)
      out(i * 2 + 1) = digits(0x0f & bytes(i))
    }

    new String(out)
  }

  def stringToBytes(string: String): Array[Byte] = {
    assert(isValidHex(string))
    val result = new Array[Byte](string.length >> 1)

    result.indices.foreach { i =>
      result(i) = (Character.digit(string(i * 2 + 1), 16) + Character.digit(string(i * 2), 16) * 16).toByte
    }

    result
  }

  def isValidHex(string: String): Boolean =
    string.length % 2 == 0 && string.toLowerCase.forall(c => c.isDigit || (c >= 'a' && c <= 'f'))
}
