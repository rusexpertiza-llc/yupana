package org.yupana.core.utils.metric

sealed trait QueryStatus extends Serializable {
  override def toString: String =
    this.getClass.getSimpleName
      .replaceAll("$", "")
      .toUpperCase
}

case object Unknown extends QueryStatus
case object Success extends QueryStatus
case class Failed(throwable: Throwable) extends QueryStatus {
  override def toString: String = s"${super.toString}:${throwable.getMessage.replaceAll(" ", "_")}"
}
