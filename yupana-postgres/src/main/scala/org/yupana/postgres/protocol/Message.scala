package org.yupana.postgres.protocol

trait Message {}

case object SSLRequest extends Message
