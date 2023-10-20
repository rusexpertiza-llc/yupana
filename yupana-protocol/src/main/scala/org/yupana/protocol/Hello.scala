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

package org.yupana.protocol

case class Hello(protocolVersion: Int, clientVersion: String, timestamp: Long, params: Map[String, String])
    extends Command[Hello](Hello)

object Hello extends MessageHelper[Hello] {
  override val tag: Byte = Tags.HELLO

  implicit override val readWrite: ReadWrite[Hello] = new ReadWrite[Hello] {
    override def read[B: Buffer](buf: B): Hello = {
      Hello(
        implicitly[ReadWrite[Int]].read(buf),
        implicitly[ReadWrite[String]].read(buf),
        implicitly[ReadWrite[Long]].read(buf),
        implicitly[ReadWrite[Map[String, String]]].read(buf)
      )
    }

    override def write[B: Buffer](buf: B, t: Hello): Unit = {
      implicitly[ReadWrite[Int]].write(buf, t.protocolVersion)
      implicitly[ReadWrite[String]].write(buf, t.clientVersion)
      implicitly[ReadWrite[Long]].write(buf, t.timestamp)
      implicitly[ReadWrite[Map[String, String]]].write(buf, t.params)
    }
  }
}
