package org.yupana.api.query

trait Transform

case class Replace(from: Seq[Expression[_]], to: Seq[Expression[_]]) extends Transform
