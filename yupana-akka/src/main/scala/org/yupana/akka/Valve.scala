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

package org.yupana.akka

import akka.stream.FanInShape.Ports
import akka.stream.{ Attributes, FanInShape2, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

import scala.collection.immutable

class Valve[T] extends GraphStage[FanInShape2[T, Int, T]] {
  val in: Inlet[T] = Inlet("in")
  val ctrl: Inlet[Int] = Inlet("control")
  val out: Outlet[T] = Outlet("out")

  override val shape: FanInShape2[T, Int, T] = new FanInShape2(Ports(out, immutable.Seq[Inlet[_]](in, ctrl)))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var limit = 0
    private var acquired = 0

    setHandler(
      ctrl,
      new InHandler {
        override def onPush(): Unit = {
          limit += grab(ctrl)
          if (acquired > 0) pull(in)
        }
      }
    )

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          limit -= 1
          acquired -= 1
          if (limit == 0) pull(ctrl)

          push(out, grab(in))
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          acquired += 1
          if (limit > 0) pull(in)
        }
      }
    )

    override def preStart(): Unit = {
      pull(ctrl)
    }
  }
}
