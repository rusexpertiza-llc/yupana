package org.yupana.netty

sealed trait Message

case class Hello(protoMajor: Byte, protoMinor: Byte) extends Message

case class SimpleQuery(sql: String) extends Message

case object Bye extends Message
