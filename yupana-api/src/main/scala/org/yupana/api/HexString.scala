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

case class HexString(bytes: Array[Byte]) {
  lazy val hex: String = HexString.bytesToString(bytes)

  override def toString: String = s"0x$hex"
}

object HexString {
  private val digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  def apply(string: String): HexString = {
    if (!HexString.isValidHex(string)) throw new IllegalArgumentException(s"$string is not valid hex value")
    new HexString(stringToBytes(string))
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
    val fixed = if (string.length % 2 == 0) string else '0' + string
    val result = new Array[Byte](fixed.length >> 1)

    result.indices.foreach { i =>
      result(i) = (Character.digit(fixed(i * 2 + 1), 16) + Character.digit(fixed(i * 2), 16) * 16).toByte
    }

    result
  }

  def isValidHex(string: String): Boolean =
    string.toLowerCase.forall(c => c.isDigit || (c >= 'a' && c <= 'f'))
}
