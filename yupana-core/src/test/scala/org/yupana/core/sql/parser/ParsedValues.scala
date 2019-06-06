package org.yupana.core.sql.parser

import fastparse.core.Parsed
import fastparse.core.Parsed.{Failure, Success}
import org.scalactic.source
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

trait ParsedValues {
  import scala.language.implicitConversions

  implicit def parsedToOpts[T, E, R](p: Parsed[T, E, R])(implicit pos: source.Position): ParsedOpts[T, E, R] = new ParsedOpts[T, E, R](p, pos)

  class ParsedOpts[T, E, R](val p: Parsed[T, E, R], pos: source.Position) {
    def value: T = p match {
      case Success(v, _) => v
      case f: Failure[E, R] =>
        throw new TestFailedException((_: StackDepthException) => Some(s"Value is not parsed ${f.msg}"), None, pos)
    }
  }

}
