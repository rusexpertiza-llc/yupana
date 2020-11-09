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

package org.yupana.api.utils

import scala.util.control.{ ControlThrowable, NonFatal }

object ResourceUtils {

  def using[T <: AutoCloseable, R](r: => T)(body: T => R): R = {
    if (r == null) throw new NullPointerException("Resource is null")

    var exception: Throwable = null
    try {
      body(r)
    } catch {
      case e: Throwable =>
        exception = e
        // We'll throw exception later
        null.asInstanceOf[R]
    } finally {
      if (exception eq null) {
        r.close()
      } else {
        try {
          r.close()
        } catch {
          case closeEx: Throwable =>
            exception = preferentiallySuppress(exception, closeEx)
        } finally {
          throw exception
        }
      }
    }
  }

  private def preferentiallySuppress(primary: Throwable, secondary: Throwable): Throwable = {
    def score(t: Throwable): Int = t match {
      case _: VirtualMachineError                   => 4
      case _: LinkageError                          => 3
      case _: InterruptedException | _: ThreadDeath => 2
      case _: ControlThrowable                      => 0
      case e if !NonFatal(e)                        => 1 // in case this method gets out of sync with NonFatal
      case _                                        => -1
    }
    @inline def suppress(t: Throwable, suppressed: Throwable): Throwable = { t.addSuppressed(suppressed); t }

    if (score(secondary) > score(primary)) suppress(secondary, primary)
    else suppress(primary, secondary)
  }
}
