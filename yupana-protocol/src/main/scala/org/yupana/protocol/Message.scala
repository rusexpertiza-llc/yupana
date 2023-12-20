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

/**
  * Base class for all messages.
  */
trait Message[M <: Message[M]] { self: M =>

  /** Instance of message helper related to the message type M */
  def helper: MessageHelper[M]

  /** Converts message to the frame */
  def toFrame[B: Buffer]: Frame = helper.toFrame(this)
}

/**
  * Helper class for a message type. Provides methods to serialize and deserialize messages.
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
  def toFrame[B: Buffer](c: M): Frame = {
    val b = implicitly[Buffer[B]]
    val buf = b.alloc(Frame.MAX_FRAME_SIZE)
    readWrite.write(buf, c)
    Frame(tag.value, b.toByteArray(buf))
  }

  /**
    * Reads message from the data frame. Checks frame type before reading.
    * @return deserialized message or None, if frame type doesn't match message type.
    */
  def readFrameOpt[B: Buffer](f: Frame): Option[M] = {
    Option.when(f.frameType == tag.value)(readFrame(f))
  }

  /**
    * Read message from the data frame
    * @param f frame to be read
    * @tparam B buffer instance
    * @return deserialized message
    */
  def readFrame[B: Buffer](f: Frame): M = {
    readWrite.read(implicitly[Buffer[B]].wrap(f.payload))
  }
}

/** Base class for client messages */
abstract class Command[C <: Message[C]](override val helper: MessageHelper[C]) extends Message[C] { self: C => }

/** Base class for server messages */
abstract class Response[R <: Response[R]](override val helper: MessageHelper[R]) extends Message[R] { self: R => }
