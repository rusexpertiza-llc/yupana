package org.yupana.api.schema

trait Schema {

  def check(expectedSchema: Array[Byte]): SchemaCheckResult
  val tables: Map[String, Table]
  def get(name: String): Option[Table]

  def toBytes: Array[Byte]
}

sealed trait SchemaCheckResult

object SchemaCheckResult {
    def empty: SchemaCheckResult = Success
    def combine(a: SchemaCheckResult, b: SchemaCheckResult): SchemaCheckResult = (a, b) match {
      case (Error(msg1), Error(msg2)) => Error(msg1 + "\n" + msg2)
      case (Error(msg1), Warning(msg2)) => Error(msg1 + "\n" + msg2)
      case (Warning(msg1), Error(msg2)) => Error(msg1 + "\n" + msg2)
      case (Warning(msg1), Warning(msg2)) => Warning(msg1 + "\n" + msg2)
      case (Success, Success) => Success
      case (Success, notSuccess) => notSuccess
      case (notSuccess, Success) => notSuccess
  }
}
case object Success extends SchemaCheckResult
case class Warning(message: String) extends SchemaCheckResult
case class Error(message: String) extends SchemaCheckResult
