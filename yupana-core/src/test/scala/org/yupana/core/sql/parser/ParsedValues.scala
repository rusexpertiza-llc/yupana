package org.yupana.core.sql.parser

import fastparse.Parsed
import fastparse.Parsed.{Failure, Success}
import org.scalactic.source
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

trait ParsedValues {
  import scala.language.implicitConversions

  implicit def parsedToOpts[T](p: Parsed[T])(implicit pos: source.Position): ParsedOpts[T] = new ParsedOpts[T](p, pos)

  class ParsedOpts[T](val p: Parsed[T], pos: source.Position) {
    def value: T = p match {
      case Success(v, _) => v
      case f: Failure =>
        throw new TestFailedException((_: StackDepthException) => Some(s"Value is not parsed ${f.msg}"), None, pos)
    }
  }

}
