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

package org.yupana.netty.protocol

import org.yupana.netty.Frame

trait Message[M <: Message[M]] { self: M =>
  def helper: MessageHelper[M]
  def toFrame: Frame = helper.toFrame(this)
}

abstract class Command[C <: Message[C]](override val helper: MessageHelper[C]) extends Message[C] { self: C => }

abstract class Response[R <: Response[R]](override val helper: MessageHelper[R]) extends Message[R] { self: R => }
