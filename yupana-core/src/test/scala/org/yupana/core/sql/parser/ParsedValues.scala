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

package org.yupana.core.sql.parser

import fastparse.Parsed
import fastparse.Parsed.{ Failure, Success }
import org.scalactic.source
import org.scalatest.exceptions.{ StackDepthException, TestFailedException }

trait ParsedValues {
  import scala.language.implicitConversions

  implicit def parsedToOpts[T](p: Parsed[T])(implicit pos: source.Position): ParsedOpts[T] = new ParsedOpts[T](p, pos)

  class ParsedOpts[T](val p: Parsed[T], pos: source.Position) {
    def value: T = p match {
      case Success(v, _) => v
      case f: Failure =>
        throw new TestFailedException((_: StackDepthException) => Some(s"Value is not parsed ${f.msg}"), None, pos)
    }

    def error: String = p match {
      case Success(v, _) =>
        throw new TestFailedException((_: StackDepthException) => Some(s"Error expected, but got $v"), None, pos)
      case f: Failure => f.msg
    }
  }

}
