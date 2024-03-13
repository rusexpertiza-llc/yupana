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

import org.yupana.api.types.ByteReaderWriter
import org.yupana.serialization.ReadWrite

/**
  * Helper class for a message type. Provides methods to serialize and deserialize messages.
  *
  * @tparam M type of the message
  */
trait MessageHelper[M <: Message[M]] {

  /** Message tag */
  val tag: Tags.Tags

  /** ReadWrite instance for the message */
  val readWrite: ReadWrite[M]

  /**
    * Serialize message into data frame.
    * @param c message to be serialized
    * @tparam B buffer instance to be used (depends on environment)
    * @return Frame containing serialized message
    */
  def toFrame[B: ByteReaderWriter](c: M, b: B): Frame[B] = {
    readWrite.write(b, c)
    Frame(tag.value, b)
  }

  /**
    * Reads message from the data frame. Checks frame type before reading.
    * @return deserialized message or None, if frame type doesn't match message type.
    */
  def readFrameOpt[B: ByteReaderWriter](f: Frame[B]): Option[M] = {
    Option.when(f.frameType == tag.value)(readFrame(f))
  }

  /**
    * Read message from the data frame
    * @param f frame to be read
    * @tparam B buffer instance
    * @return deserialized message
    */
  def readFrame[B: ByteReaderWriter](f: Frame[B]): M = {
    readWrite.read(f.payload)
  }
}
